package jobworkerdb

import (
	"bytes"
	"context"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/domonda/go-types/uu"
)

// nulByte is the zero byte that PostgreSQL accepts in neither text nor jsonb values.
const nulByte = "\x00"

// escapeLen is the length of a \uXXXX JSON escape sequence.
const escapeLen = 6

// maxExtraStripRounds bounds how many times sanitizeJSON re-strips escapes after
// a removal formed a new one. Two is comfortably above what any real input needs
// (one extra round covers a raw byte removed out of the middle of an escape), and
// it keeps the work linear against input crafted to chain removals forever.
const maxExtraStripRounds = 2

// sanitizeString makes user provided text storable in a PostgreSQL text column
// by removing invalid UTF-8 byte sequences and zero bytes, neither of which
// PostgreSQL can store. Clean strings are returned unchanged.
//
// What this removes is part of the on-disk data contract, not just a write-time
// filter: the origin-keyed delete methods sanitize their lookup key so it matches
// what insertJob stored, so changing the rules here orphans rows written by an
// earlier version from deletes issued by a later one.
func sanitizeString(s string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(s, ""), nulByte, "")
}

// sanitizeJSON makes user provided JSON storable in a PostgreSQL jsonb column.
// Beyond the raw bytes sanitizeString removes, it strips the \uXXXX escape
// sequences that jsonb rejects (see stripJSONUnstorableEscapes) — these matter
// because they are valid JSON that encoding/json and json.Valid both accept, so
// nothing upstream rejects them and only the insert would fail.
//
// The type parameter preserves the passed JSON type (notnull.JSON,
// nullable.JSON) so that its driver.Valuer implementation still applies when the
// result is passed to a query as an any argument.
//
// Any empty result is returned as a nil T, whether the argument was already
// empty or sanitizing emptied it, because PostgreSQL rejects a zero-length value
// as jsonb input while nil is handled by both JSON types: nullable.JSON binds it
// as NULL, notnull.JSON binds it as the empty object.
//
// Unmarshalling and re-marshalling through encoding/json would remove both escape
// classes on its own and replace all of this, but it is not an option: a JSON
// number round-tripped through any becomes a float64, silently corrupting integer
// payload fields beyond 2^53, and object key order is not preserved.
func sanitizeJSON[T ~[]byte](j T) T {
	if len(j) == 0 {
		return nil
	}
	// Job payloads can be large and are almost always already storable, so the
	// clean case must not copy. Each step below returns or scans in place, so a
	// clean payload allocates nothing.
	sanitized := stripJSONUnstorableEscapes(j)
	if bytes.IndexByte(sanitized, 0) >= 0 || !utf8.Valid(sanitized) {
		// Do NOT "simplify" this by cleaning raw bytes BEFORE stripping escapes.
		// That is not the same transformation: a raw bad byte between two otherwise
		// well-formed surrogate halves leaves both unpaired here and removes them,
		// whereas cleaning raw bytes first re-pairs them into a kept character.
		//
		// This ordering is not a general guarantee that garbage never becomes a
		// valid character — bad bytes INSIDE each half make both escapes
		// unrecognizable to the pass above, and cleaning them still joins the halves
		// into a pair. It only fixes the case where the halves themselves are intact.
		sanitized = []byte(sanitizeString(string(sanitized)))
	}
	// Removing bytes can join a backslash to the text behind it and so form an
	// unstorable escape that was not in the input. Two ways in: dropping a raw byte
	// out of `\`+0xFF+`u0000` leaves a zero byte escape behind, and dropping an
	// escape can close the gap in a malformed `\u00` that is followed by a real
	// zero byte escape and then `00`: the leading `\u00` is skipped as
	// unrecognizable, and removing the escape behind it joins it to that `00`.
	//
	// So strip again after a removal — but a BOUNDED number of times, not until
	// stable. Each seam can be made to re-form an escape, and crafted malformed
	// input can chain those seams so that every round exposes exactly one more:
	// `\u00`×n + a zero byte escape + `00`×n needs about len/6 rounds, making an
	// unbounded loop quadratic in both scanning and allocation, on the hot path of
	// every job write and (via AddJobBundle) with a transaction open.
	//
	// A small cap keeps that bounded. Nothing legitimate needs more: valid JSON
	// cannot chain at all, because every backslash there is followed by a complete
	// escape, so no partial escape is ever left in front of a removal. Input that
	// is still unstorable after the cap is malformed JSON, which PostgreSQL rejects
	// on its own — so the write fails loudly instead of burning CPU to repair
	// something it would refuse anyway.
	for round := 0; round < maxExtraStripRounds && len(sanitized) != len(j); round++ {
		stripped := stripJSONUnstorableEscapes(sanitized)
		if len(stripped) == len(sanitized) {
			break
		}
		sanitized = stripped
	}
	if len(sanitized) == 0 {
		return nil
	}
	return T(sanitized)
}

// sanitizedString sanitizes text for a PostgreSQL text column and reports what it
// removed. Callers use this instead of sanitizeString plus a separate log call so
// that the removed-byte count is always measured against the value passed in,
// even when the caller substitutes a fallback for an emptied result afterwards.
func sanitizedString(ctx context.Context, idKey string, id uu.ID, field, s string) string {
	sanitized := sanitizeString(s)
	logSanitized(ctx, idKey, id, field, len(s), len(sanitized))
	return sanitized
}

// sanitizedJSON sanitizes JSON for a PostgreSQL jsonb column and reports what it
// removed, as sanitizedString does for text.
func sanitizedJSON[T ~[]byte](ctx context.Context, idKey string, id uu.ID, field string, j T) T {
	sanitized := sanitizeJSON(j)
	logSanitized(ctx, idKey, id, field, len(j), len(sanitized))
	return sanitized
}

// logSanitized reports that sanitizing actually removed something from a value
// about to be written. Sanitizing only ever deletes, so a shorter result is
// exactly the signal that the stored value differs from what the caller passed.
//
// Without this the divergence leaves no trace at all: the producer's in-memory Job
// keeps the original bytes, the row holds a shortened value that is usually still
// well-formed and plausible, and nothing connects the two when someone later asks
// why a stored identifier is missing a character. WARN rather than DEBUG because
// losing user data is not routine — a deployment where this fires steadily has an
// upstream encoding problem worth fixing at the source.
func logSanitized(ctx context.Context, idKey string, id uu.ID, field string, before, after int) {
	if before == after {
		return
	}
	log.WarnCtx(ctx, "Removed characters PostgreSQL cannot store").
		UUID(idKey, id).
		Str("field", field).
		Int("removedBytes", before-after).
		Log()
}

// stripJSONUnstorableEscapes removes the \uXXXX escape sequences that
// PostgreSQL's jsonb input rejects even though they are valid JSON:
//
//   - The zero byte escape, rejected as "unsupported Unicode escape sequence".
//     encoding/json emits it for a zero byte inside a Go string, so a marshalled
//     payload carries no raw zero byte to strip — only this escape.
//   - Unpaired surrogates, rejected as "Unicode low surrogate must follow a high
//     surrogate": a high surrogate not followed by a low one, or a low surrogate
//     not preceded by a high one. Valid surrogate pairs are kept intact.
//
// j is returned as is, without copying, when there is nothing to remove.
func stripJSONUnstorableEscapes(j []byte) []byte {
	var (
		out   []byte
		start int // start of the run not yet copied into out
	)
	for i := 0; i < len(j); {
		next := bytes.IndexByte(j[i:], '\\')
		if next < 0 {
			break
		}
		i += next

		u, isCodeUnit := jsonEscapedCodeUnit(j[i:])
		if !isCodeUnit {
			// Any other escape: skip the backslash together with the character it
			// escapes, so an escaped backslash cannot be misread as the start of
			// the escape sequence that follows it.
			i += 2
			continue
		}
		switch {
		case u == 0:
			// Unstorable, dropped below.
		case utf16.IsSurrogate(u):
			// DecodeRune yields a real code point only for a high surrogate
			// followed by a low one, so this both tests the pair and rejects a
			// surrogate that stands alone in either half's range.
			if low, ok := jsonEscapedCodeUnit(j[i+escapeLen:]); ok &&
				utf16.DecodeRune(u, low) != unicode.ReplacementChar {
				i += 2 * escapeLen // A complete pair, keep both halves.
				continue
			}
			// Unpaired surrogate, dropped below.
		default:
			i += escapeLen // An ordinary escaped code point, keep it.
			continue
		}
		if out == nil {
			out = make([]byte, 0, len(j))
		}
		out = append(out, j[start:i]...)
		i += escapeLen
		start = i
	}
	if out == nil {
		return j
	}
	return append(out, j[start:]...)
}

// jsonEscapedCodeUnit decodes a leading \uXXXX escape sequence into the UTF-16
// code unit it encodes. The hex digits are case insensitive, as JSON allows, so
// matching the escape as a literal string would miss the uppercase spelling that
// producers other than encoding/json emit.
func jsonEscapedCodeUnit(j []byte) (u rune, ok bool) {
	if len(j) < escapeLen || j[0] != '\\' || j[1] != 'u' {
		return 0, false
	}
	for _, c := range j[2:escapeLen] {
		d := hexDigit(c)
		if d < 0 {
			return 0, false
		}
		u = u<<4 | d
	}
	return u, true
}

// hexDigit returns the value of the hex digit c, or -1 if c is not a hex digit.
func hexDigit(c byte) rune {
	switch {
	case c >= '0' && c <= '9':
		return rune(c - '0')
	case c >= 'a' && c <= 'f':
		return rune(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return rune(c-'A') + 10
	}
	return -1
}
