package jobqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/notnull"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
)

// marshalerOK / marshalerErr exercise payloads with their own MarshalJSON
// method (a custom type that is none of the concrete byte/string cases), which
// NewJob marshals with encoding/json.
type marshalerOK struct{}

func (marshalerOK) MarshalJSON() ([]byte, error) { return []byte(`{"ok":true}`), nil }

type marshalerErr struct{}

func (marshalerErr) MarshalJSON() ([]byte, error) { return nil, errors.New("marshal boom") }

// marshalerEmpty / marshalerInvalid / marshalerNull return output without an
// error that NewJob must still reject: no JSON text, invalid JSON and null.
type marshalerEmpty struct{}

func (marshalerEmpty) MarshalJSON() ([]byte, error) { return []byte{}, nil }

type marshalerInvalid struct{}

func (marshalerInvalid) MarshalJSON() ([]byte, error) { return []byte(`{invalid`), nil }

type marshalerNull struct{}

func (marshalerNull) MarshalJSON() ([]byte, error) { return []byte(" null "), nil }

// marshalerCanceled returns context.Canceled, an error with meaning to DoJob.
type marshalerCanceled struct{}

func (marshalerCanceled) MarshalJSON() ([]byte, error) { return nil, context.Canceled }

// structPayload exercises the default (reflection-marshalled) case.
type structPayload struct {
	Name string
}

const testJobID = "11111111-2222-3333-4444-555555555555"

func TestNewJobWithPriority_ValidPayloads(t *testing.T) {
	id := uu.IDFrom(testJobID)
	tests := []struct {
		name     string
		payload  any
		wantJSON string
	}{
		{"notnull.JSON", notnull.JSON(`{"a":1}`), `{"a":1}`},
		{"nullable.JSON", nullable.JSON(`{"a":1}`), `{"a":1}`},
		{"json.RawMessage", json.RawMessage(`{"a":1}`), `{"a":1}`},
		{"byte slice", []byte(`{"a":1}`), `{"a":1}`},
		{"string", `{"a":1}`, `{"a":1}`},
		{"json.Marshaler", marshalerOK{}, `{"ok":true}`},
		{"default reflect-marshal struct", structPayload{Name: "x"}, `{"Name":"x"}`},
		{"default reflect-marshal map", map[string]int{"a": 1}, `{"a":1}`},
		{"default reflect-marshal int", 42, `42`},
		{"pointer to struct", &structPayload{Name: "x"}, `{"Name":"x"}`},
		{"pointer to notnull.JSON", new(notnull.JSON(`{"a":1}`)), `{"a":1}`},
		{"pointer to nullable.JSON", new(nullable.JSON(`{"a":1}`)), `{"a":1}`},
		{"pointer to json.RawMessage", new(json.RawMessage(`{"a":1}`)), `{"a":1}`},
		{"null nested in an object", `{"a":null}`, `{"a":null}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := jobqueue.NewJobWithPriority(id, "jobType", "origin", tt.payload, 0, nullable.Time{})
			require.NoError(t, err)
			require.NotNil(t, job)
			assert.JSONEq(t, tt.wantJSON, string(job.Payload))
		})
	}
}

func TestNewJobWithPriority_InvalidPayloads(t *testing.T) {
	const (
		invalidJSON    = "invalid job payload: not valid JSON"
		emptyPayload   = "invalid job payload: empty"
		nullPayload    = "invalid job payload: JSON null"
		cantMarshal    = "can't be marshalled to JSON"
		marshalsToNull = "marshals to JSON null"
	)
	id := uu.IDFrom(testJobID)
	tests := []struct {
		name    string
		payload any
		wantErr string // pins which rejection fired
	}{
		{"invalid notnull.JSON", notnull.JSON(`{invalid`), invalidJSON},
		{"invalid nullable.JSON", nullable.JSON(`{invalid`), invalidJSON},
		{"invalid json.RawMessage", json.RawMessage(`{invalid`), invalidJSON},
		{"invalid byte slice", []byte(`{invalid`), invalidJSON},
		{"invalid string", `{invalid`, invalidJSON},
		// Empty input holds no JSON text. go-types v1.0.0-beta.2 made
		// notnull.JSON.Valid report it as valid (it binds as {}), so these
		// rows pin that NewJob still rejects it instead of queuing a job
		// whose worker would run on an all-zero payload.
		{"empty notnull.JSON", notnull.JSON(""), emptyPayload},
		{"nil notnull.JSON", notnull.JSON(nil), emptyPayload},
		{"nil nullable.JSON", nullable.JSON(nil), emptyPayload},
		{"nil json.RawMessage", json.RawMessage(nil), emptyPayload},
		{"empty byte slice", []byte{}, emptyPayload},
		{"empty string", "", emptyPayload},
		// A pointer to a JSON type is dereferenced instead of marshalled:
		// notnull.JSON.MarshalJSON would turn the empty value into {}.
		{"pointer to empty notnull.JSON", new(notnull.JSON), emptyPayload},
		{"pointer to nil nullable.JSON", new(nullable.JSON), emptyPayload},
		{"pointer to nil json.RawMessage", new(json.RawMessage), emptyPayload},
		{"json.Marshaler returning error", marshalerErr{}, cantMarshal},
		{"json.Marshaler returning empty output", marshalerEmpty{}, cantMarshal},
		{"json.Marshaler returning invalid JSON", marshalerInvalid{}, cantMarshal},
		{"json.Marshaler returning null", marshalerNull{}, marshalsToNull},
		{"default unmarshalable value", make(chan int), cantMarshal},
		// A nil pointer is a nil payload, even when its type has a value
		// receiver MarshalJSON that would panic when called through it.
		{"nil *notnull.JSON", (*notnull.JSON)(nil), "invalid job payload: nil *notnull.JSON"},
		{"nil *nullable.JSON", (*nullable.JSON)(nil), "invalid job payload: nil *nullable.JSON"},
		{"nil struct pointer", (*structPayload)(nil), "invalid job payload: nil *jobqueue_test.structPayload"},
		// A JSON null payload carries no data for the worker, which would
		// run on a zero value just like with an empty payload.
		{"null string", "null", nullPayload},
		{"null with surrounding whitespace", " null\n", nullPayload},
		{"null notnull.JSON", notnull.JSON("null"), nullPayload},
		{"null nullable.JSON", nullable.JSON("null"), nullPayload},
		{"pointer to null nullable.JSON", new(nullable.JSON("null")), nullPayload},
		{"nil map", map[string]int(nil), marshalsToNull},
		{"nil slice", []int(nil), marshalsToNull},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := jobqueue.NewJobWithPriority(id, "jobType", "origin", tt.payload, 0, nullable.Time{})
			require.ErrorIs(t, err, jobqueue.ErrInvalidPayload)
			require.ErrorContains(t, err, tt.wantErr)
			assert.Nil(t, job)
		})
	}
}

// TestNewJobWithPriority_ErrorRootKeepsReason pins that errs.Root of a payload
// error, which DoJob uses as a failed job's log title, is the reason and not the
// bare "invalid job payload" of the ErrInvalidPayload sentinel, also when a
// MarshalJSON error caused it.
func TestNewJobWithPriority_ErrorRootKeepsReason(t *testing.T) {
	_, err := jobqueue.NewJobWithPriority(uu.IDFrom(testJobID), "jobType", "origin", "null", 0, nullable.Time{})
	require.ErrorIs(t, err, jobqueue.ErrInvalidPayload)
	assert.Equal(t, "invalid job payload: JSON null", errs.Root(err).Error())

	_, err = jobqueue.NewJobWithPriority(uu.IDFrom(testJobID), "jobType", "origin", marshalerErr{}, 0, nullable.Time{})
	require.ErrorIs(t, err, jobqueue.ErrInvalidPayload)
	assert.Contains(t, errs.Root(err).Error(), "invalid job payload: jobqueue_test.marshalerErr can't be marshalled to JSON")
}

// TestNewJobWithPriority_ErrorHidesCauseIdentity pins that a MarshalJSON error is
// not wrapped: a context.Canceled from it would otherwise make DoJob treat a
// permanent rejection as a cancellation and reset the job without end.
func TestNewJobWithPriority_ErrorHidesCauseIdentity(t *testing.T) {
	_, err := jobqueue.NewJobWithPriority(uu.IDFrom(testJobID), "jobType", "origin", marshalerCanceled{}, 0, nullable.Time{})
	require.ErrorIs(t, err, jobqueue.ErrInvalidPayload)
	assert.NotErrorIs(t, err, context.Canceled)
}

// TestNewJobWithPriority_ErrorsOmitPayload pins that a rejected payload is not
// quoted in the error, which callers log: it can be large or carry personal data.
func TestNewJobWithPriority_ErrorsOmitPayload(t *testing.T) {
	const secret = "jane.doe@example.com"
	_, err := jobqueue.NewJobWithPriority(uu.IDFrom(testJobID), "jobType", "origin", `{"email":"`+secret, 0, nullable.Time{})
	require.ErrorContains(t, err, "invalid job payload: not valid JSON")
	assert.NotContains(t, err.Error(), secret)
}

func TestNewJobWithPriority_ArgErrors(t *testing.T) {
	id := uu.IDFrom(testJobID)

	t.Run("empty jobType", func(t *testing.T) {
		job, err := jobqueue.NewJobWithPriority(id, "", "origin", `{}`, 0, nullable.Time{})
		require.Error(t, err)
		assert.Nil(t, job)
	})

	t.Run("nil payload", func(t *testing.T) {
		job, err := jobqueue.NewJobWithPriority(id, "jobType", "origin", nil, 0, nullable.Time{})
		require.ErrorIs(t, err, jobqueue.ErrInvalidPayload)
		require.ErrorContains(t, err, "invalid job payload: nil")
		assert.Nil(t, job)
	})

	t.Run("nil payload is reported before an empty jobType", func(t *testing.T) {
		job, err := jobqueue.NewJobWithPriority(id, "", "origin", nil, 0, nullable.Time{})
		require.ErrorIs(t, err, jobqueue.ErrInvalidPayload)
		require.ErrorContains(t, err, "invalid job payload: nil")
		assert.Nil(t, job)
	})
}

func TestNewJobWithPriority_Fields(t *testing.T) {
	id := uu.IDFrom(testJobID)
	startAt := nullable.TimeFrom(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))

	job, err := jobqueue.NewJobWithPriority(id, "jobType", "origin", `{"a":1}`, 7, startAt, 3)
	require.NoError(t, err)

	assert.Equal(t, id, job.ID)
	assert.Equal(t, "jobType", job.Type)
	assert.Equal(t, "origin", job.Origin)
	assert.Equal(t, int64(7), job.Priority)
	assert.Equal(t, startAt, job.StartAt)
	assert.Equal(t, 3, job.MaxRetryCount, "first variadic retryCount sets MaxRetryCount")
	assert.Zero(t, job.CurrentRetryCount)
	assert.False(t, job.CreatedAt.IsZero())
	assert.Equal(t, job.CreatedAt, job.UpdatedAt, "CreatedAt and UpdatedAt are stamped together")
}

func TestNewJobWithPriority_DefaultRetryCount(t *testing.T) {
	id := uu.IDFrom(testJobID)
	job, err := jobqueue.NewJobWithPriority(id, "jobType", "origin", `{}`, 0, nullable.Time{})
	require.NoError(t, err)
	assert.Zero(t, job.MaxRetryCount, "no variadic retryCount leaves MaxRetryCount at zero")
}

func TestNewJob(t *testing.T) {
	id := uu.IDFrom(testJobID)
	job, err := jobqueue.NewJob(id, "jobType", "origin", `{}`, nullable.Time{}, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(0), job.Priority, "NewJob delegates with priority 0")
	assert.Equal(t, 2, job.MaxRetryCount)
}

func TestNewJobReflectType(t *testing.T) {
	id := uu.IDFrom(testJobID)
	payload := structPayload{Name: "x"}

	job, err := jobqueue.NewJobReflectType(id, "origin", payload, nullable.Time{})
	require.NoError(t, err)
	assert.Equal(t, jobqueue.ReflectJobTypeOfPayload(payload), job.Type,
		"job type is derived from the payload type via reflection")
	assert.JSONEq(t, `{"Name":"x"}`, string(job.Payload))
}

func TestNewJobReflectType_NilPayload(t *testing.T) {
	// The job type is derived from the payload type, which a nil payload
	// lacks: it must be reported as an error, not panic.
	job, err := jobqueue.NewJobReflectType(uu.IDFrom(testJobID), "origin", nil, nullable.Time{})
	require.ErrorIs(t, err, jobqueue.ErrInvalidPayload)
	require.ErrorContains(t, err, "invalid job payload: nil")
	assert.Nil(t, job)
}
