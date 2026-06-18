package jobqueue_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-types/notnull"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
)

// marshalerOK / marshalerErr exercise the json.Marshaler case of the payload
// type switch (a custom type that is none of the concrete byte/string cases).
type marshalerOK struct{}

func (marshalerOK) MarshalJSON() ([]byte, error) { return []byte(`{"ok":true}`), nil }

type marshalerErr struct{}

func (marshalerErr) MarshalJSON() ([]byte, error) { return nil, errors.New("marshal boom") }

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
	id := uu.IDFrom(testJobID)
	tests := []struct {
		name    string
		payload any
	}{
		{"invalid notnull.JSON", notnull.JSON(`{invalid`)},
		{"invalid nullable.JSON", nullable.JSON(`{invalid`)},
		{"invalid json.RawMessage", json.RawMessage(`{invalid`)},
		{"invalid byte slice", []byte(`{invalid`)},
		{"invalid string", `{invalid`},
		{"json.Marshaler returning error", marshalerErr{}},
		{"default unmarshalable value", make(chan int)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := jobqueue.NewJobWithPriority(id, "jobType", "origin", tt.payload, 0, nullable.Time{})
			require.Error(t, err)
			assert.Nil(t, job)
		})
	}
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
		require.Error(t, err)
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
