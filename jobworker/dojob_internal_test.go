package jobworker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-errs"

	"github.com/domonda/go-jobqueue"
)

// TestDoJobCanceledJobSkipsOnErrorAndLog verifies the cancellation guard in DoJob:
// a worker that returns a context.Canceled error (directly or wrapped) is an
// expected interruption, so DoJob must NOT invoke OnError or log it, yet it must
// still return the error so doJobAndSaveResultInDB can detect the cancellation and
// reset/retry the job. A genuine failure is the control: OnError still fires.
func TestDoJobCanceledJobSkipsOnErrorAndLog(t *testing.T) {
	resetWorkerRegistryState(t)

	// OnError is a package-global callback; capture invocations and restore it.
	origOnError := OnError
	t.Cleanup(func() { OnError = origOnError })
	var onErrorCalls []error
	OnError = func(err error) { onErrorCalls = append(onErrorCalls, err) }

	cases := []struct {
		name        string
		workerErr   error
		wantOnError bool
		wantCancel  bool
	}{
		{"direct context.Canceled", context.Canceled, false, true},
		{"wrapped context.Canceled", errs.Errorf("dialing backend: %w", context.Canceled), false, true},
		{"genuine failure", errs.New("boom"), true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			onErrorCalls = nil
			jobType := JobType("cancel-guard-" + tc.name)
			Register(jobType, func(context.Context, *jobqueue.Job) (any, error) {
				return nil, tc.workerErr
			})

			job := &jobqueue.Job{Type: jobType}
			err := DoJob(t.Context(), job)

			// The error is always returned so the caller can act on it.
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.workerErr)

			if tc.wantCancel {
				// The worker-thread guard relies on context.Canceled surviving the
				// wrapping DoJob applies to its returned error.
				assert.ErrorIs(t, err, context.Canceled,
					"cancellation must propagate out of DoJob")
			}

			if tc.wantOnError {
				assert.Len(t, onErrorCalls, 1, "OnError must fire for a genuine failure")
			} else {
				assert.Empty(t, onErrorCalls, "OnError must be skipped for a cancelled job")
			}
		})
	}
}
