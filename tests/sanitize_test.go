package tests

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/notnull"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
)

// rawNul is a raw zero byte, which PostgreSQL stores in neither text nor jsonb.
const rawNul = "\x00"

// jsonEscapedNul is the JSON escape sequence for a zero byte, written with a
// doubled backslash so that Go does not turn it into a raw zero byte in source.
// This is the form encoding/json produces for a zero byte inside a Go string,
// and it is rejected by jsonb even though it is valid JSON.
const jsonEscapedNul = "\\u0000"

// invalidUTF8 is a byte that cannot appear in valid UTF-8.
var invalidUTF8 = string([]byte{0xff})

// loneSurrogate is an unpaired high surrogate escape, written with a doubled
// backslash like jsonEscapedNul. It is valid JSON that Go accepts but jsonb
// refuses, which is the second escape class the sanitizer has to remove.
// Browsers' JSON.stringify emits these for a truncated emoji.
const loneSurrogate = "\\ud83d"

// TestPostgresRejectsUnstorableStrings is the negative control for the
// sanitization tests below: it pins down what PostgreSQL actually refuses, so
// those tests are demonstrably exercising a real constraint rather than passing
// vacuously. If PostgreSQL ever started accepting these, the sanitizing could be
// dropped — and this test would tell us.
func TestPostgresRejectsUnstorableStrings(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	t.Run("text rejects a raw zero byte", func(t *testing.T) {
		err := db.Exec(t.Context(),
			/*sql*/ `select $1::text`, "a"+rawNul+"b")
		require.Error(t, err, "PostgreSQL text cannot hold a zero byte")
	})

	t.Run("jsonb rejects an escaped zero byte", func(t *testing.T) {
		err := db.Exec(t.Context(),
			/*sql*/ `select $1::jsonb`, `{"a":"`+jsonEscapedNul+`"}`)
		require.Error(t, err, "jsonb rejects escaped zero bytes, not just raw ones")
		assert.Contains(t, err.Error(), "unsupported Unicode escape sequence")
	})

	t.Run("text rejects invalid UTF-8", func(t *testing.T) {
		err := db.Exec(t.Context(),
			/*sql*/ `select $1::text`, "a"+invalidUTF8+"b")
		require.Error(t, err, "PostgreSQL rejects bytes that are not valid UTF-8")
	})

	t.Run("empty is not valid jsonb", func(t *testing.T) {
		err := db.Exec(t.Context(),
			/*sql*/ `select $1::jsonb`, "")
		require.Error(t, err, "an emptied JSON value must be bound as NULL, not empty")
	})

	t.Run("jsonb rejects an unpaired surrogate escape", func(t *testing.T) {
		raw := `{"a":"` + loneSurrogate + `"}`
		require.True(t, json.Valid([]byte(raw)),
			"Go considers this valid JSON, so nothing upstream of the insert rejects it")

		err := db.Exec(t.Context(),
			/*sql*/ `select $1::jsonb`, raw)
		require.Error(t, err, "jsonb refuses surrogates that are not part of a pair")
		// PostgreSQL explains this one ("Unicode low surrogate must follow a high
		// surrogate") in the DETAIL field, which the driver does not surface, so
		// only the primary message is available to assert on.
		assert.Contains(t, err.Error(), "invalid input syntax for type json")
	})
}

// TestAddJobSanitizesUserProvidedStrings verifies that a job carrying user
// provided data which PostgreSQL cannot store is still persisted instead of
// failing the insert. Without sanitizing, a single zero byte anywhere in a
// payload makes AddJob fail and the job is never queued at all.
func TestAddJobSanitizesUserProvidedStrings(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000041")
	jobType := "test-sanitize-type"

	// A Go payload struct whose string field holds a zero byte and an invalid
	// UTF-8 byte, which is how this reaches the queue in practice: encoding/json
	// escapes the zero byte, so the marshalled payload trips jsonb's escape check.
	payload := struct {
		Text string `json:"text"`
	}{
		Text: "before" + rawNul + "middle" + invalidUTF8 + "after",
	}
	origin := "test-origin" + rawNul + invalidUTF8

	job, err := jobqueue.NewJob(jobID, jobType, origin, payload, nullable.Time{})
	require.NoError(t, err)
	require.Contains(t, string(job.Payload), jsonEscapedNul,
		"the marshalled payload must contain the escape sequence for this test to be meaningful")

	require.NoError(t, jobqueue.Add(t.Context(), job))
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	// Sanitizing happens at the DB write boundary only. The caller's Job must come
	// back out of Add byte-identical, as the package docs promise, so that callers
	// comparing against their own struct are not surprised.
	assert.Contains(t, job.Origin, rawNul, "Add must not sanitize the caller's Job in place")
	assert.Contains(t, string(job.Payload), jsonEscapedNul, "Add must not sanitize the caller's payload in place")

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)

	assert.Equal(t, "test-origin", stored.Origin, "origin keeps its storable text")
	assert.NotContains(t, string(stored.Payload), jsonEscapedNul)
	// Deliberately no assertion that the stored payload holds no RAW zero byte:
	// encoding/json emits the escape rather than a raw byte and jsonb cannot store
	// one either way, so such a check passes even against a sanitizer that does
	// nothing. The escape assertion above is what actually exercises this path.

	var out struct {
		Text string `json:"text"`
	}
	require.NoError(t, stored.Payload.UnmarshalTo(&out))
	assert.Equal(t, "beforemiddle�after", out.Text,
		"only the unstorable zero byte is dropped; the rest of the payload survives")

	assert.Equal(t, jobType, stored.Type, "the dispatch key must be stored verbatim")
}

// TestAddJobSanitizesUnpairedSurrogates covers the second class of escape that
// jsonb rejects. This one arrives only through a pass-through raw JSON payload
// (notnull.JSON / json.RawMessage / string), because Go's own marshaller never
// emits an unpaired surrogate — but jobqueue.NewJob validates such a payload with
// json.Valid, which accepts it, so before sanitizing handled it the job was
// created and then lost at the insert.
func TestAddJobSanitizesUnpairedSurrogates(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000049")

	// A truncated emoji next to a complete one: the unpaired half must go, the
	// valid pair must survive.
	raw := `{"broken":"` + loneSurrogate + `","intact":"😀"}`
	job, err := jobqueue.NewJob(jobID, "test-sanitize-surrogate-type", "test", notnull.JSON(raw), nullable.Time{})
	require.NoError(t, err, "NewJob accepts it because json.Valid does")

	require.NoError(t, jobqueue.Add(t.Context(), job),
		"the job must be stored instead of lost at the insert")
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)

	var out struct {
		Broken string `json:"broken"`
		Intact string `json:"intact"`
	}
	require.NoError(t, stored.Payload.UnmarshalTo(&out))
	assert.Empty(t, out.Broken, "the unpaired surrogate is dropped")
	assert.Equal(t, "😀", out.Intact, "a valid surrogate pair must survive intact")
}

// TestAddJobRejectsEntirelyUnstorablePayload pins the one place sanitizing must
// NOT rescue the write. notnull.JSON binds a nil value as the empty object, so a
// payload with nothing storable left would otherwise be stored as {} and the job
// would be claimed and dispatched to a worker that unmarshals an all-zero payload
// struct and acts on it. Failing loudly matches how an emptied origin and an
// unstorable Type already behave.
func TestAddJobRejectsEntirelyUnstorablePayload(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000054")

	// Bypasses NewJob's json.Valid check the way a struct-literal caller would.
	job := &jobqueue.Job{
		ID:      jobID,
		Type:    "test-sanitize-empty-payload-type",
		Origin:  "test",
		Payload: notnull.JSON(rawNul + invalidUTF8),
	}
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	require.Error(t, jobqueue.Add(t.Context(), job),
		"a payload with nothing storable left must not be queued")

	_, err := jobqueue.GetJob(t.Context(), jobID)
	assert.Error(t, err, "no row may have been written")
}

// TestSetJobErrorSanitizesErrorStrings verifies that a job failure is recorded
// even when the error text or error data carries bytes PostgreSQL cannot store.
// Error messages routinely embed the user data that caused the failure, so an
// unsanitized message would make SetJobError itself fail — losing the error and
// leaving the job stuck in-progress with its bundle never completing.
func TestSetJobErrorSanitizesErrorStrings(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	dbAPI := dataBaseAPI(t)

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000042")
	job, err := jobqueue.NewJob(jobID, "test-sanitize-error-type", "test", "{}", nullable.Time{})
	require.NoError(t, err)
	require.NoError(t, jobqueue.Add(t.Context(), job))
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	errorMsg := "failed to parse" + rawNul + " input" + invalidUTF8
	errorData := nullable.JSON(`{"detail":"bad` + jsonEscapedNul + `byte"}`)

	require.NoError(t, dbAPI.SetJobError(t.Context(), jobID, errorMsg, errorData))

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	require.True(t, stored.HasError(), "the error must actually be recorded")

	assert.Equal(t, "failed to parse input", stored.ErrorMsg.Get())
	assert.NotContains(t, string(stored.ErrorData), jsonEscapedNul)
	assert.Equal(t, `{"detail": "badbyte"}`, strings.ReplaceAll(string(stored.ErrorData), "\n", ""))
}

// TestSetJobErrorNeverStoresAnEmptyMessage is a regression test for a state
// confusion: error_msg is a plain text column, so SQL reads an empty string as errored
// (GetAllJobsWithErrors uses `error_msg is not null`), but Go reads it back
// through nullable.NonEmptyString, whose IsNull is `== ""`. An error message
// sanitized down to nothing would therefore make a job that actually FAILED
// report Succeeded() == true, while DeleteFinishedJobs (`error_msg is null`)
// would never reap the row.
func TestSetJobErrorNeverStoresAnEmptyMessage(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	dbAPI := dataBaseAPI(t)

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000046")
	job, err := jobqueue.NewJob(jobID, "test-sanitize-empty-error-type", "test", "{}", nullable.Time{})
	require.NoError(t, err)
	require.NoError(t, jobqueue.Add(t.Context(), job))
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	// Every character of this message is unstorable, so sanitizing empties it.
	require.NoError(t, dbAPI.SetJobError(t.Context(), jobID, rawNul+invalidUTF8, nullable.JSON(nil)))

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.True(t, stored.HasError(), "a job that failed must still report an error")
	assert.False(t, stored.Succeeded(), "a failed job must never report success")
	assert.NotEmpty(t, stored.ErrorMsg.Get(), "a placeholder must replace the emptied message")
}

// TestSetJobResultEntirelyUnstorable pins the ordering of sanitizing and the
// empty-result fallback. A result with nothing storable left must become the
// empty object so the job still counts as finished, rather than failing the
// update and leaving the job — and its bundle — stuck.
func TestSetJobResultEntirelyUnstorable(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	dbAPI := dataBaseAPI(t)

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000047")
	job, err := jobqueue.NewJob(jobID, "test-sanitize-empty-result-type", "test", "{}", nullable.Time{})
	require.NoError(t, err)
	require.NoError(t, jobqueue.Add(t.Context(), job))
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	require.NoError(t, dbAPI.SetJobResult(t.Context(), jobID, nullable.JSON(rawNul+invalidUTF8)))

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.False(t, stored.HasError())
	assert.Equal(t, "{}", strings.TrimSpace(string(stored.Result)))
}

// TestDeleteJobsFromOriginDoesNotNormalizeTheKey pins the asymmetry between the
// write path and the lookup path. Sanitizing is not injective, so normalizing a
// destructive unbounded key would let an origin containing unstorable characters
// collapse onto a different, legitimate origin and delete its jobs — after any
// authorization the caller performed on the raw string. Deleting too little is
// recoverable; deleting jobs the caller never named is not.
//
// So a job stored under a sanitized origin is reachable by the sanitized string,
// and a delete issued with the original dirty string must not reach it.
func TestDeleteJobsFromOriginDoesNotNormalizeTheKey(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	dbAPI := dataBaseAPI(t)

	const storableOrigin = "sanitize-delete-origin"
	dirtyOrigin := storableOrigin + rawNul + invalidUTF8

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000048")
	job, err := jobqueue.NewJob(jobID, "test-sanitize-delete-type", dirtyOrigin, "{}", nullable.Time{})
	require.NoError(t, err)
	require.NoError(t, jobqueue.Add(t.Context(), job))
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	require.Equal(t, storableOrigin, stored.Origin, "the origin was sanitized on write")

	// The dirty string must not delete the row it collapses onto. PostgreSQL
	// refuses the zero byte in a text parameter, so this cannot even be sent —
	// which is exactly the point: an unstorable key matches nothing.
	assert.Error(t, dbAPI.DeleteJobsFromOrigin(t.Context(), dirtyOrigin),
		"a delete key is never normalized, so an unstorable one cannot match")

	_, err = jobqueue.GetJob(t.Context(), jobID)
	assert.NoError(t, err, "the job must survive a delete issued with the dirty origin")

	// The sanitized string is how the row is actually reachable.
	require.NoError(t, dbAPI.DeleteJobsFromOrigin(t.Context(), storableOrigin))
	_, err = jobqueue.GetJob(t.Context(), jobID)
	assert.Error(t, err, "deleting by the stored origin must work")
}

// TestDeleteJobBundlesFromOriginDoesNotNormalizeTheKey mirrors the job case for
// bundles, where over-deleting is worse still because the delete cascades to every
// job in the bundle.
func TestDeleteJobBundlesFromOriginDoesNotNormalizeTheKey(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	dbAPI := dataBaseAPI(t)

	const storableOrigin = "sanitize-delete-bundle-origin"
	dirtyOrigin := storableOrigin + rawNul + invalidUTF8

	bundleID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000050")
	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000051")

	job, err := jobqueue.NewJob(jobID, "test-sanitize-delete-bundle-job-type", dirtyOrigin, "{}", nullable.Time{})
	require.NoError(t, err)
	require.NoError(t, jobqueue.AddBundle(t.Context(), &jobqueue.JobBundle{
		ID:      bundleID,
		Type:    "test-sanitize-delete-bundle-type",
		Origin:  dirtyOrigin,
		NumJobs: 1,
		Jobs:    []*jobqueue.Job{job},
	}))
	t.Cleanup(func() {
		bg := context.Background()
		_ = jobqueue.GetService(bg).DeleteJobBundle(bg, bundleID)
		_ = jobqueue.DeleteJob(bg, jobID)
	})

	assert.Error(t, dbAPI.DeleteJobBundlesFromOrigin(t.Context(), dirtyOrigin),
		"an unstorable delete key must not be normalized onto a real origin")

	_, err = jobqueue.GetJobBundle(t.Context(), bundleID)
	assert.NoError(t, err, "the bundle must survive a delete issued with the dirty origin")

	require.NoError(t, dbAPI.DeleteJobBundlesFromOrigin(t.Context(), storableOrigin))
	_, err = jobqueue.GetJobBundle(t.Context(), bundleID)
	assert.Error(t, err, "deleting by the stored origin must work")
}

// TestSetJobErrorOnBundledJobWithUnstorableData covers the end-to-end consequence
// this change exists to prevent. If SetJobError fails on unstorable error data,
// the whole transaction rolls back — including the bundle's num_jobs_stopped
// increment — so the bundle can never reach completion and waits forever. Every
// other SetJobError sanitize test uses a standalone job, leaving the bundle
// counter path with unstorable data unexercised.
func TestSetJobErrorOnBundledJobWithUnstorableData(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	bundleID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000052")
	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000053")

	job, err := jobqueue.NewJob(jobID, "test-sanitize-bundled-error-type", "test", "{}", nullable.Time{})
	require.NoError(t, err)
	require.NoError(t, jobqueue.AddBundle(t.Context(), &jobqueue.JobBundle{
		ID:      bundleID,
		Type:    "test-sanitize-bundled-error-bundle-type",
		Origin:  "test",
		NumJobs: 1,
		Jobs:    []*jobqueue.Job{job},
	}))
	t.Cleanup(func() {
		bg := context.Background()
		_ = jobqueue.GetService(bg).DeleteJobBundle(bg, bundleID)
		_ = jobqueue.DeleteJob(bg, jobID)
	})

	// Both the message and the error data are entirely unstorable, so this is the
	// worst case for the write.
	require.NoError(t, dataBaseAPI(t).SetJobError(
		t.Context(),
		jobID,
		rawNul+invalidUTF8,
		nullable.JSON(rawNul+invalidUTF8),
	), "the failure must be recordable even when nothing in it is storable")

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.True(t, stored.HasError(), "the job must still be marked errored")

	bundle, err := jobqueue.GetJobBundle(t.Context(), bundleID)
	require.NoError(t, err)
	assert.Equal(t, 1, bundle.NumJobsStopped,
		"the bundle must count the failure, or it can never complete")
}

// TestSetJobResultSanitizesResult verifies that a successful job's result is
// stored even when the worker returned strings PostgreSQL cannot hold. A failing
// SetJobResult would leave a job that actually succeeded looking unfinished and
// block its bundle from ever completing.
func TestSetJobResultSanitizesResult(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	dbAPI := dataBaseAPI(t)

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000043")
	job, err := jobqueue.NewJob(jobID, "test-sanitize-result-type", "test", "{}", nullable.Time{})
	require.NoError(t, err)
	require.NoError(t, jobqueue.Add(t.Context(), job))
	t.Cleanup(func() { _ = jobqueue.DeleteJob(context.Background(), jobID) })

	result := nullable.JSON(`{"text":"ok` + jsonEscapedNul + `"}`)
	require.NoError(t, dbAPI.SetJobResult(t.Context(), jobID, result))

	stored, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.False(t, stored.HasError())
	assert.NotContains(t, string(stored.Result), jsonEscapedNul)
	assert.Equal(t, `{"text": "ok"}`, strings.ReplaceAll(string(stored.Result), "\n", ""))
}

// TestAddJobBundleSanitizesOrigin verifies the bundle insert is sanitized too.
// The bundle row is written in the same transaction as its jobs, so an
// unstorable bundle origin would roll back and discard every job in the bundle.
func TestAddJobBundleSanitizesOrigin(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	bundleID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000044")
	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000045")

	job, err := jobqueue.NewJob(jobID, "test-sanitize-bundle-job-type", "test", "{}", nullable.Time{})
	require.NoError(t, err)

	bundle := &jobqueue.JobBundle{
		ID:      bundleID,
		Type:    "test-sanitize-bundle-type",
		Origin:  "bundle-origin" + rawNul + invalidUTF8,
		NumJobs: 1,
		Jobs:    []*jobqueue.Job{job},
	}
	require.NoError(t, jobqueue.AddBundle(t.Context(), bundle))
	t.Cleanup(func() {
		bg := context.Background()
		_ = jobqueue.GetService(bg).DeleteJobBundle(bg, bundleID)
		_ = jobqueue.DeleteJob(bg, jobID)
	})

	stored, err := jobqueue.GetJobBundle(t.Context(), bundleID)
	require.NoError(t, err)
	assert.Equal(t, "bundle-origin", stored.Origin)
	assert.Len(t, stored.Jobs, 1, "the bundle's jobs must have been committed with it")
}
