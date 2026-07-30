package jobworkerdb

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-types/notnull"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
	"github.com/domonda/golog"
)

// invalidUTF8 is a byte that cannot appear in valid UTF-8.
const invalidUTF8 = "\xff"

// escapedNul is the JSON escape sequence for a zero byte. It is written with a
// doubled backslash because Go would otherwise interpret it as a zero rune in
// the source, which is exactly the raw form this constant must NOT be.
const escapedNul = "\\u0000"

// Surrogate escape sequences, written with a doubled backslash for the same
// reason as escapedNul. hi and lo together spell one emoji; alone each is an
// unpaired surrogate that jsonb refuses. hiUpper is the same code unit as hi in
// the uppercase hex spelling that JSON also allows.
const (
	hi      = "\\ud83d"
	lo      = "\\ude00"
	hiUpper = "\\uD83D"
	escA    = "\\u0041" // an ordinary, storable escaped code point
)

// TestSanitizeString verifies that text a job carries from user provided data is
// reduced to what a PostgreSQL text column can actually store, because otherwise
// the whole insert fails and the job is silently lost. Legitimate non-ASCII text
// must survive untouched — dropping umlauts would corrupt real job data.
func TestSanitizeString(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"clean ASCII is unchanged":                 {"hello world", "hello world"},
		"empty stays empty":                        {"", ""},
		"valid multi-byte UTF-8 is preserved":      {"Grüße 日本 🎉", "Grüße 日本 🎉"},
		"raw zero byte is removed":                 {"a" + nulByte + "b", "ab"},
		"multiple zero bytes are removed":          {nulByte + "a" + nulByte + nulByte + "b" + nulByte, "ab"},
		"invalidUTF8 UTF-8 is removed":             {"a" + invalidUTF8 + "b", "ab"},
		"invalidUTF8 UTF-8 run is removed":         {"a" + string([]byte{0xff, 0xfe, 0xc3}) + "b", "ab"},
		"zero byte and invalidUTF8 UTF-8 together": {"a" + nulByte + invalidUTF8 + "b", "ab"},
		"an entirely unstorable string empties":    {nulByte + invalidUTF8, ""},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.want, sanitizeString(tt.input))
		})
	}
}

// TestSanitizeJSON verifies that a job payload, result, or error data survives
// the trip into a jsonb column. jsonb is stricter than the JSON spec: it rejects
// escaped zero bytes as well as raw ones, so a sanitizer that only removes raw
// bytes would still let inserts fail.
func TestSanitizeJSON(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"clean JSON is unchanged":            {`{"a":1,"b":"x"}`, `{"a":1,"b":"x"}`},
		"multi-byte UTF-8 is preserved":      {`{"a":"Grüße 🎉"}`, `{"a":"Grüße 🎉"}`},
		"raw zero byte is removed":           {`{"a":"x` + nulByte + `y"}`, `{"a":"xy"}`},
		"invalidUTF8 UTF-8 is removed":       {`{"a":"x` + invalidUTF8 + `y"}`, `{"a":"xy"}`},
		"escaped zero byte is removed":       {`{"a":"x` + escapedNul + `y"}`, `{"a":"xy"}`},
		"only an escaped zero byte":          {`{"a":"` + escapedNul + `"}`, `{"a":""}`},
		"several escaped zero bytes":         {`{"a":"` + escapedNul + escapedNul + `b"}`, `{"a":"b"}`},
		"escaped zero byte in a JSON key":    {`{"k` + escapedNul + `":1}`, `{"k":1}`},
		"other escape sequences survive":     {`{"a":"x\ny\tz\"qä"}`, `{"a":"x\ny\tz\"qä"}`},
		"escaped backslash is not misparsed": {`{"a":"\\u0000"}`, `{"a":"\\u0000"}`},

		// A surrogate followed by six non-escape bytes exercises the lookahead
		// rejecting a non-backslash while it still has room for a full escape.
		"unpaired high surrogate before plain text": {`{"a":"` + hi + `yyyyyy"}`, `{"a":"yyyyyy"}`},
		// Mirror of "pair kept, stray high dropped": a stray half BEFORE a valid pair
		// advances the copy position into the buffer and the pair must survive that.
		"stray high dropped, pair kept":      {`{"a":"` + hi + hi + lo + `"}`, `{"a":"` + hi + lo + `"}`},
		"escaped backslash then real escape": {`{"a":"\\` + escapedNul + `"}`, `{"a":"\\"}`},

		// jsonb rejects unpaired surrogates as well, with a different error than
		// the zero byte escape. Go's json.Valid accepts them, so nothing upstream
		// catches them and only the insert would fail.
		"unpaired high surrogate removed":  {`{"a":"x` + hi + `y"}`, `{"a":"xy"}`},
		"unpaired low surrogate removed":   {`{"a":"x` + lo + `y"}`, `{"a":"xy"}`},
		"uppercase spelling also removed":  {`{"a":"x` + hiUpper + `y"}`, `{"a":"xy"}`},
		"valid surrogate pair is kept":     {`{"a":"` + hi + lo + `"}`, `{"a":"` + hi + lo + `"}`},
		"pair kept, stray high dropped":    {`{"a":"` + hi + lo + hi + `"}`, `{"a":"` + hi + lo + `"}`},
		"low then high is both unpaired":   {`{"a":"` + lo + hi + `"}`, `{"a":""}`},
		"escaped backslash before a pair":  {`{"a":"\\` + hi + lo + `"}`, `{"a":"\\` + hi + lo + `"}`},
		"ordinary escaped code point kept": {`{"a":"` + escA + `"}`, `{"a":"` + escA + `"}`},

		// A zero byte escape between the two halves breaks the pair, so all three
		// escapes are unstorable on their own and all three go. Dropping the zero
		// byte escape does NOT re-pair the halves around it.
		"nulByte escape between halves unpairs both": {`{"a":"` + hi + escapedNul + lo + `"}`, `{"a":""}`},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := sanitizeJSON(notnull.JSON(tt.input))
			assert.Equal(t, tt.want, string(got))
			assert.True(t, json.Valid(got), "sanitizing must not break JSON validity")
		})
	}
}

// TestStripJSONUnstorableEscapesMalformedTail covers input that ends mid-escape.
// Error data and payloads are not guaranteed to be well-formed JSON by the time
// they reach sanitizing, so a truncated escape must neither panic nor be dropped.
func TestStripJSONUnstorableEscapesMalformedTail(t *testing.T) {
	cases := map[string]string{
		"trailing lone backslash":     `{"a":"x` + escapedNul + `\`,
		"only a backslash":            `\`,
		"truncated escape at the end": `{"a":"x` + escapedNul + `\u00`,
		"high surrogate then cut off": `{"a":"` + hi + `\u`,
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			// The assertion that matters is that this returns at all: the loop must
			// not index past the end of a truncated escape sequence.
			got := string(stripJSONUnstorableEscapes([]byte(input)))
			assert.NotContains(t, got, escapedNul, "the complete escape is still removed")
		})
	}

	// A \u whose digits are not all hex is not an escape sequence at all, so it
	// must be left alone rather than mistaken for an unstorable code unit. This is
	// malformed JSON, hence not in the table above, which asserts JSON validity.
	t.Run("non-hex digits are not an escape", func(t *testing.T) {
		malformed := `{"a":"\uZZZZ"}`
		require.False(t, json.Valid([]byte(malformed)), "the input is already malformed JSON")
		assert.Equal(t, malformed, string(stripJSONUnstorableEscapes([]byte(malformed))))
	})
}

// TestSanitizeJSONEscapeFormedByByteRemoval is the regression test for the
// ordering trap between the two sanitizing steps. Escape stripping runs first, so
// it skips `\`+0xFF as an escaped 0xFF; removing the 0xFF afterwards then joins
// the backslash to the text behind it and forms an escape that was never in the
// input. Without a second stripping pass the result is valid JSON carrying the
// very escape jsonb refuses, so the write fails on a value that was sanitized.
func TestSanitizeJSONEscapeFormedByByteRemoval(t *testing.T) {
	tests := map[string]struct {
		input string
		want  string
	}{
		"zero byte escape formed by removal":       {`{"a":"\` + invalidUTF8 + `u0000"}`, `{"a":""}`},
		"high surrogate escape formed by removal":  {`{"a":"\` + invalidUTF8 + `ud83d"}`, `{"a":""}`},
		"zero byte joins across a removed nulByte": {`{"a":"\` + nulByte + `u0000"}`, `{"a":""}`},
		// The backslash is itself escaped here, so removing the byte after it must
		// leave literal u0000 text rather than forming an escape.
		"escaped backslash keeps literal text": {`{"a":"\\` + invalidUTF8 + `u0041"}`, `{"a":"\\u0041"}`},
		// A raw bad byte among the hex digits makes the sequence unrecognizable to
		// the first pass (hexDigit rejects it), and removing the byte then joins the
		// remaining digits into the very escape jsonb refuses.
		"escape formed inside a broken hex escape": {`{"a":"\u0` + invalidUTF8 + `000"}`, `{"a":""}`},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := sanitizeJSON(notnull.JSON(tt.input))
			assert.Equal(t, tt.want, string(got))
			assert.True(t, json.Valid(got))
		})
	}
}

// TestSanitizeJSONStripsUntilStable pins that sanitizing repeats until nothing
// changes, rather than assuming a fixed number of passes. A malformed escape whose
// hex digits swallow the backslash of a following real escape is skipped as
// unrecognizable; removing that following escape then joins the leftover digits
// into a NEW unstorable escape. A single extra pass happens to fix the raw-byte
// variant of this but not this one, because the input has no raw byte and is valid
// UTF-8, so the raw-byte branch that used to trigger the extra pass never runs.
func TestSanitizeJSONStripsUntilStable(t *testing.T) {
	bs := string([]byte{'\\'})

	// Bytes: \ u 0 0 \ u 0 0 0 0 0 0 — the first escape's "digits" include a backslash.
	input := `{"a":"` + bs + `u00` + escapedNul + `00"}`
	require.False(t, json.Valid([]byte(input)), "the input is already malformed JSON")

	// One pass alone leaves a freshly formed zero byte escape behind.
	afterOnePass := string(stripJSONUnstorableEscapes([]byte(input)))
	require.Contains(t, afterOnePass, escapedNul,
		"a single pass must be shown to be insufficient, or this test proves nothing")

	got := string(sanitizeJSON(notnull.JSON(input)))
	assert.NotContains(t, got, escapedNul, "sanitizing must not leave an escape jsonb rejects")
	assert.Equal(t, `{"a":""}`, got)
}

// TestSanitizeJSONBoundedOnChainedRemovals guards the hot path against crafted
// input. Every removal seam can be made to re-form an escape, and those seams can
// be chained so each round exposes exactly one more. Stripping until stable is
// therefore quadratic in both scanning and allocation — measured at 108ms for a
// 30KB payload before the round cap, extrapolating to minutes for a 1MB one, with
// a transaction held open when the write came through AddJobBundle.
//
// This asserts the work stays linear. Such input is malformed JSON that PostgreSQL
// rejects anyway, so leaving it unrepaired is the correct fail-closed outcome.
func TestSanitizeJSONBoundedOnChainedRemovals(t *testing.T) {
	bs := string([]byte{'\\'})
	chain := func(n int) string {
		return `{"a":"` + strings.Repeat(bs+`u00`, n) + escapedNul + strings.Repeat("00", n) + `"}`
	}

	small, large := chain(200), chain(2000)
	require.False(t, json.Valid([]byte(large)), "the chain is malformed JSON by construction")

	elapsed := func(input string) time.Duration {
		start := time.Now()
		sanitizeJSON(notnull.JSON(input))
		return time.Since(start)
	}
	// Warm up so the first call's costs don't skew the ratio.
	elapsed(small)

	smallTime, largeTime := elapsed(small), elapsed(large)
	ratio := float64(largeTime) / float64(max(smallTime, time.Nanosecond))

	// 10x the input must not cost anywhere near 100x the time. A generous ceiling
	// keeps this from flaking on a loaded machine while still failing hard if the
	// round cap is removed: unbounded stripping put this ratio near 100.
	assert.Less(t, ratio, 30.0,
		"10x input took %v vs %v (%.1fx) — sanitizing must stay bounded, not quadratic",
		largeTime, smallTime, ratio)
}

// TestStripJSONUnstorableEscapesNoCopyWhenClean verifies the allocation-free
// contract: clean JSON must come back as the same backing array, because every
// job write runs through here and payloads can be large.
func TestStripJSONUnstorableEscapesNoCopyWhenClean(t *testing.T) {
	clean := []byte(`{"a":"` + escA + hi + lo + `","b":"plain"}`)
	got := stripJSONUnstorableEscapes(clean)
	require.Equal(t, string(clean), string(got))
	assert.Same(t, &clean[0], &got[0], "clean JSON must not be copied")
}

// TestSanitizedWrappersReportRemovedBytes pins the WARN line that makes an
// otherwise silent lossy write traceable. Without it a shortened stored value
// leaves no trace at all: the producer's in-memory Job still holds the original
// bytes and the row holds a plausible-looking shorter one.
//
// It also pins the reason the wrappers exist instead of a bare sanitize call plus
// a separate log call: the removed-byte count is measured against the value passed
// IN, so a caller substituting a fallback for an emptied result afterwards (as
// SetJobError does) cannot hide the removal. And it pins the field/id labels,
// which are plain string literals at six call sites where a copy-paste would
// otherwise be invisible.
func TestSanitizedWrappersReportRemovedBytes(t *testing.T) {
	var logged bytes.Buffer
	restore := log
	log = golog.NewLogger(golog.NewConfig(
		&golog.DefaultLevels,
		golog.AllLevelsActive,
		golog.NewJSONWriterConfig(&logged, golog.NewDefaultFormat()),
	))
	t.Cleanup(func() { log = restore })

	id := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000060")

	t.Run("a clean value logs nothing", func(t *testing.T) {
		logged.Reset()
		got := sanitizedString(t.Context(), "jobID", id, "origin", "already storable")
		assert.Equal(t, "already storable", got)
		assert.Empty(t, logged.String(),
			"a healthy deployment must not be flooded with WARNs for clean data")
	})

	t.Run("an emptied string reports every removed byte", func(t *testing.T) {
		logged.Reset()
		got := sanitizedString(t.Context(), "jobID", id, "error message", nulByte+invalidUTF8)
		require.Empty(t, got)

		line := logged.String()
		assert.Contains(t, line, `"WARN"`)
		assert.Contains(t, line, `"field":"error message"`)
		assert.Contains(t, line, `"jobID":"`+id.String()+`"`)
		assert.Contains(t, line, `"removedBytes":2`,
			"the count must reflect the value passed in, not a later fallback")
	})

	t.Run("JSON reports under the bundle id key too", func(t *testing.T) {
		logged.Reset()
		got := sanitizedJSON(t.Context(), "jobBundleID", id, "payload", notnull.JSON(`{"a":"x`+escapedNul+`"}`))
		assert.Equal(t, `{"a":"x"}`, string(got))

		line := logged.String()
		assert.Contains(t, line, `"jobBundleID":"`+id.String()+`"`)
		assert.Contains(t, line, `"field":"payload"`)
		assert.Contains(t, line, `"removedBytes":6`)
	})
}

// TestSanitizeJSONPreservesNull verifies that sanitizing does not turn a NULL
// nullable.JSON into a non-NULL empty value. Job result and error data columns
// are nullable, and an empty non-NULL value is both invalidUTF8 jsonb input and a
// different state than NULL.
func TestSanitizeJSONPreservesNull(t *testing.T) {
	var null nullable.JSON
	require.True(t, null.IsNull())

	got := sanitizeJSON(null)
	assert.True(t, got.IsNull(), "a NULL nullable.JSON must stay NULL")
	assert.Nil(t, []byte(got))
}

// TestSanitizeJSONNeverReturnsZeroLength verifies that no input produces a
// non-nil zero-length result. PostgreSQL rejects a zero-length value as jsonb, so
// binding one would fail the write — and inside SetJobError that failure rolls
// back the transaction, skipping the bundle's num_jobs_stopped increment and
// leaving the bundle unable to ever complete. nil is safe for both JSON types.
func TestSanitizeJSONNeverReturnsZeroLength(t *testing.T) {
	inputs := map[string]string{
		"already empty but non-nil": "",
		"emptied by sanitizing":     nulByte + invalidUTF8,
		"only an escaped zero byte": escapedNul,
	}
	for name, input := range inputs {
		t.Run(name, func(t *testing.T) {
			require.False(t, nullable.JSON(input).IsNull(),
				"the input itself must not already be NULL, or this proves nothing")

			got := sanitizeJSON(nullable.JSON(input))
			assert.Nil(t, []byte(got), "must be nil, not a zero-length slice")
			assert.True(t, got.IsNull())
		})
	}
}

// TestSanitizeJSONEmptiedBindsPerType pins what the nil returned for an emptied
// value actually binds as, which is the whole reason sanitizeJSON is generic over
// the JSON type instead of taking a []byte. The two types deliberately disagree:
// notnull.JSON turns nil into the empty object, so an emptied payload is stored
// for a `payload jsonb not null` column, while nullable.JSON turns nil into SQL
// NULL, which is what the nullable result and error_data columns want. Collapsing
// the parameter to []byte would silently store NULL in the not-null column.
func TestSanitizeJSONEmptiedBindsPerType(t *testing.T) {
	entirelyUnstorable := nulByte + invalidUTF8

	notNullValue, err := sanitizeJSON(notnull.JSON(entirelyUnstorable)).Value()
	require.NoError(t, err)
	assert.Equal(t, []byte("{}"), notNullValue, "an emptied notnull.JSON must bind as the empty object")

	nullableValue, err := sanitizeJSON(nullable.JSON(entirelyUnstorable)).Value()
	require.NoError(t, err)
	assert.Nil(t, nullableValue, "an emptied nullable.JSON must bind as SQL NULL")
}

// TestSanitizeJSONOfMarshalledGoString is the regression test for the case that
// actually reaches the database: a Go payload struct holding a zero byte. Go's
// encoding/json turns that byte into an escape sequence rather than a raw byte,
// so the marshalled JSON contains no zero byte at all and only escape-aware
// sanitizing keeps jsonb from rejecting it.
func TestSanitizeJSONOfMarshalledGoString(t *testing.T) {
	payload := struct {
		Text string `json:"text"`
	}{
		Text: "before" + nulByte + "after" + invalidUTF8,
	}

	marshalled, err := notnull.MarshalJSON(payload)
	require.NoError(t, err)
	require.NotContains(t, string(marshalled), nulByte, "encoding/json escapes the zero byte instead of emitting it raw")
	require.Contains(t, string(marshalled), escapedNul, "so the escape sequence is what has to be removed")

	sanitized := sanitizeJSON(marshalled)
	assert.NotContains(t, string(sanitized), escapedNul)
	assert.NotContains(t, string(sanitized), nulByte)
	assert.True(t, json.Valid(sanitized))

	var out struct {
		Text string `json:"text"`
	}
	require.NoError(t, json.Unmarshal(sanitized, &out))
	assert.Equal(t, "beforeafter�", out.Text, "surrounding text must survive; encoding/json already replaced the invalidUTF8 byte")
}
