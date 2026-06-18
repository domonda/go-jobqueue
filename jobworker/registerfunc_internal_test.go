package jobworker

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/domonda/go-types/notnull"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
)

// rfPayload / rfResult are the worker argument and result types used by the
// reflection-registration tests.
type rfPayload struct {
	Name string
}

type rfResult struct {
	Greeting string
}

// getRegisteredWorker returns the WorkerFunc closure that registerFunc wrapped
// and stored for jobType, so the runtime behavior of that closure can be
// exercised directly without a database or worker threads.
func getRegisteredWorker(t *testing.T, jobType string) WorkerFunc {
	t.Helper()
	workersMtx.RLock()
	defer workersMtx.RUnlock()
	w := workers[jobType]
	require.NotNil(t, w, "a worker for %q must be registered", jobType)
	return w
}

// TestRegisterFuncPanics covers every signature-validation panic branch of
// registerFunc, reached through the exported RegisterFunc.
func TestRegisterFuncPanics(t *testing.T) {
	cases := []struct {
		name string
		fn   any
	}{
		{"not a function", 42},
		{"no arguments", func() {}},
		{"two args without context", func(int, int) {}},
		{"context but no payload arg", func(context.Context) {}},
		{"context with too many args", func(context.Context, int, int) {}},
		{"arg not JSON-marshalable", func(chan int) {}},
		{"three results", func(int) (int, int, int) { return 0, 0, 0 }},
		{"two results, first not JSON-marshalable", func(int) (chan int, error) { return nil, nil }},
		{"two results, second not error", func(int) (int, int) { return 0, 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetWorkerRegistryState(t)
			assert.Panics(t, func() { RegisterFunc(tc.fn) })
		})
	}
}

// TestRegisterFuncForJobTypeEmptyPanics covers the empty-jobType guard that is
// specific to RegisterFuncForJobType.
func TestRegisterFuncForJobTypeEmptyPanics(t *testing.T) {
	resetWorkerRegistryState(t)
	assert.Panics(t, func() { RegisterFuncForJobType("", func(rfPayload) {}) })
}

// TestRegisterFuncReflectJobType verifies RegisterFunc derives the job type from
// the (dereferenced) payload type, for both value and pointer arguments.
func TestRegisterFuncReflectJobType(t *testing.T) {
	want := jobqueue.JobTypeOfPayloadType(reflect.TypeFor[rfPayload]())

	t.Run("value argument", func(t *testing.T) {
		resetWorkerRegistryState(t)
		RegisterFunc(func(context.Context, rfPayload) error { return nil })
		assert.True(t, IsRegistered(want))
	})

	t.Run("pointer argument dereferences to the same type", func(t *testing.T) {
		resetWorkerRegistryState(t)
		RegisterFunc(func(*rfPayload) error { return nil })
		assert.True(t, IsRegistered(want))
	})
}

// TestRegisterFuncRuntime exercises the wrapped WorkerFunc closure: payload
// unmarshalling, pointer-vs-value argument construction, the with/without
// context call paths, and each result-arity extraction.
func TestRegisterFuncRuntime(t *testing.T) {
	t.Run("context, value arg, (result, error)", func(t *testing.T) {
		resetWorkerRegistryState(t)
		RegisterFuncForJobType("t", func(_ context.Context, p rfPayload) (rfResult, error) {
			return rfResult{Greeting: "hi " + p.Name}, nil
		})
		result, err := getRegisteredWorker(t, "t")(t.Context(), &jobqueue.Job{Payload: notnull.JSON(`{"Name":"bob"}`)})
		require.NoError(t, err)
		assert.Equal(t, rfResult{Greeting: "hi bob"}, result)
	})

	t.Run("no context, value arg, no results", func(t *testing.T) {
		resetWorkerRegistryState(t)
		called := false
		var got rfPayload
		RegisterFuncForJobType("t", func(p rfPayload) {
			called = true
			got = p
		})
		result, err := getRegisteredWorker(t, "t")(t.Context(), &jobqueue.Job{Payload: notnull.JSON(`{"Name":"x"}`)})
		require.NoError(t, err)
		assert.Nil(t, result)
		assert.True(t, called)
		assert.Equal(t, rfPayload{Name: "x"}, got)
	})

	t.Run("pointer arg receives a non-nil pointer", func(t *testing.T) {
		resetWorkerRegistryState(t)
		var gotPtr *rfPayload
		RegisterFuncForJobType("t", func(p *rfPayload) error {
			gotPtr = p
			return nil
		})
		_, err := getRegisteredWorker(t, "t")(t.Context(), &jobqueue.Job{Payload: notnull.JSON(`{"Name":"y"}`)})
		require.NoError(t, err)
		require.NotNil(t, gotPtr)
		assert.Equal(t, "y", gotPtr.Name)
	})

	t.Run("single error result, nil error", func(t *testing.T) {
		resetWorkerRegistryState(t)
		RegisterFuncForJobType("t", func(rfPayload) error { return nil })
		result, err := getRegisteredWorker(t, "t")(t.Context(), &jobqueue.Job{Payload: notnull.JSON(`{}`)})
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("single error result, non-nil error", func(t *testing.T) {
		resetWorkerRegistryState(t)
		RegisterFuncForJobType("t", func(rfPayload) error { return errors.New("worker failed") })
		result, err := getRegisteredWorker(t, "t")(t.Context(), &jobqueue.Job{Payload: notnull.JSON(`{}`)})
		require.Error(t, err)
		assert.Nil(t, result, "an error result is returned as err, not result")
	})

	t.Run("single non-error result", func(t *testing.T) {
		resetWorkerRegistryState(t)
		RegisterFuncForJobType("t", func(p rfPayload) rfResult { return rfResult{Greeting: p.Name} })
		result, err := getRegisteredWorker(t, "t")(t.Context(), &jobqueue.Job{Payload: notnull.JSON(`{"Name":"z"}`)})
		require.NoError(t, err)
		assert.Equal(t, rfResult{Greeting: "z"}, result)
	})

	t.Run("payload that does not unmarshal returns an error", func(t *testing.T) {
		resetWorkerRegistryState(t)
		RegisterFuncForJobType("t", func(rfPayload) {})
		// A JSON array cannot be unmarshalled into the struct argument.
		_, err := getRegisteredWorker(t, "t")(t.Context(), &jobqueue.Job{Payload: notnull.JSON(`[1,2,3]`)})
		require.Error(t, err)
	})
}
