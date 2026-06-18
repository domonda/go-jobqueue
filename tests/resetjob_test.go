package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
)

func TestResetJobDecrementsBundleCounter(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	// pqconn uses a global listener per connection URL, so calling
	// ListenOnChannel again without unlistening first fails with
	// "channel is already open".
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	t.Run("ResetJob decrements num_jobs_stopped for counted bundle jobs", func(t *testing.T) {
		jobType := "f47ac10b-58cc-4372-a567-000000000001"

		var shouldSucceed atomic.Bool
		jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
			if shouldSucceed.Load() {
				return "ok", nil
			}
			return nil, errors.New("intentional failure")
		})
		t.Cleanup(func() { jobworker.Unregister(jobType) })

		bundleID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000001")
		job1ID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000011")
		job2ID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000012")

		job1, err := jobqueue.NewJob(job1ID, jobType, "test-reset", "{}", nullable.Time{})
		require.NoError(t, err)
		job2, err := jobqueue.NewJob(job2ID, jobType, "test-reset", "{}", nullable.Time{})
		require.NoError(t, err)

		bundle := &jobqueue.JobBundle{
			ID:      bundleID,
			Type:    "test-reset-bundle",
			Origin:  "test",
			NumJobs: 2,
			Jobs:    []*jobqueue.Job{job1, job2},
		}
		err = jobqueue.AddBundle(t.Context(), bundle)
		require.NoError(t, err)

		t.Cleanup(func() {
			bgCtx := context.Background()
			// Bundle may be auto-deleted after success; ignore errors
			jobqueue.GetService(bgCtx).DeleteJobBundle(bgCtx, bundleID)
			jobqueue.DeleteJob(bgCtx, job1ID)
			jobqueue.DeleteJob(bgCtx, job2ID)
		})

		// Phase 1: Process jobs — both fail (retryCount=0, so they're final failures)
		err = jobworker.StartThreads(t.Context(), 1)
		require.NoError(t, err)

		waiter1 := NewJobWaiter(t.Context(), t, job1ID)
		waiter2 := NewJobWaiter(t.Context(), t, job2ID)
		require.NoError(t, waiter1.Wait())
		require.NoError(t, waiter2.Wait())

		jobworker.FinishThreads(context.Background())

		// Verify both jobs failed
		j1, err := jobqueue.GetJob(t.Context(), job1ID)
		require.NoError(t, err)
		require.True(t, j1.HasError(), "job1 should have error")

		j2, err := jobqueue.GetJob(t.Context(), job2ID)
		require.NoError(t, err)
		require.True(t, j2.HasError(), "job2 should have error")

		// Verify bundle counter was incremented for both final failures
		b, err := jobqueue.GetJobBundle(t.Context(), bundleID)
		require.NoError(t, err)
		assert.Equal(t, 2, b.NumJobsStopped, "both failed jobs should be counted in bundle")

		// Phase 2: Reset both jobs — counter must be decremented
		err = jobqueue.ResetJob(t.Context(), job1ID)
		require.NoError(t, err)
		err = jobqueue.ResetJob(t.Context(), job2ID)
		require.NoError(t, err)

		b, err = jobqueue.GetJobBundle(t.Context(), bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "counter should be decremented after ResetJob")

		// Verify jobs were actually reset
		j1, err = jobqueue.GetJob(t.Context(), job1ID)
		require.NoError(t, err)
		assert.False(t, j1.Started(), "job1 should not be started after reset")
		assert.False(t, j1.Stopped(), "job1 should not be stopped after reset")
		assert.False(t, j1.HasError(), "job1 should not have error after reset")

		// Phase 3: Process again — jobs succeed this time
		// Without the fix, SetJobResult would try to increment num_jobs_stopped
		// past num_jobs, violating the CHECK constraint.
		shouldSucceed.Store(true)

		err = jobworker.StartThreads(t.Context(), 1)
		require.NoError(t, err)
		t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })

		// Wait for the bundle to complete (auto-deleted on success)
		bundleWaiter := &Waiter{
			Check: func() bool {
				_, err := jobqueue.GetJobBundle(t.Context(), bundleID)
				// Bundle deleted by auto-cleanup means all jobs succeeded
				return err != nil
			},
			Timeout:       5 * time.Second,
			PollFrequency: 50 * time.Millisecond,
		}
		require.NoError(t, bundleWaiter.Wait(), "bundle should complete without CHECK constraint violation")
	})

	t.Run("ResetJobs decrements num_jobs_stopped for counted bundle jobs", func(t *testing.T) {
		jobType := "f47ac10b-58cc-4372-a567-000000000002"

		var shouldSucceed atomic.Bool
		jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
			if shouldSucceed.Load() {
				return "ok", nil
			}
			return nil, errors.New("intentional failure")
		})
		t.Cleanup(func() { jobworker.Unregister(jobType) })

		bundleID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000002")
		job1ID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000021")
		job2ID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000022")

		job1, err := jobqueue.NewJob(job1ID, jobType, "test-reset", "{}", nullable.Time{})
		require.NoError(t, err)
		job2, err := jobqueue.NewJob(job2ID, jobType, "test-reset", "{}", nullable.Time{})
		require.NoError(t, err)

		bundle := &jobqueue.JobBundle{
			ID:      bundleID,
			Type:    "test-reset-bundle",
			Origin:  "test",
			NumJobs: 2,
			Jobs:    []*jobqueue.Job{job1, job2},
		}
		err = jobqueue.AddBundle(t.Context(), bundle)
		require.NoError(t, err)

		t.Cleanup(func() {
			bgCtx := context.Background()
			jobqueue.GetService(bgCtx).DeleteJobBundle(bgCtx, bundleID)
			jobqueue.DeleteJob(bgCtx, job1ID)
			jobqueue.DeleteJob(bgCtx, job2ID)
		})

		// Phase 1: Both jobs fail
		err = jobworker.StartThreads(t.Context(), 1)
		require.NoError(t, err)

		waiter1 := NewJobWaiter(t.Context(), t, job1ID)
		waiter2 := NewJobWaiter(t.Context(), t, job2ID)
		require.NoError(t, waiter1.Wait())
		require.NoError(t, waiter2.Wait())

		jobworker.FinishThreads(context.Background())

		b, err := jobqueue.GetJobBundle(t.Context(), bundleID)
		require.NoError(t, err)
		require.Equal(t, 2, b.NumJobsStopped, "both failed jobs should be counted")

		// Phase 2: Reset both jobs in one call
		err = jobqueue.ResetJobs(t.Context(), uu.IDSlice{job1ID, job2ID})
		require.NoError(t, err)

		b, err = jobqueue.GetJobBundle(t.Context(), bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "counter should be decremented after ResetJobs")

		// Phase 3: Jobs succeed
		shouldSucceed.Store(true)

		err = jobworker.StartThreads(t.Context(), 1)
		require.NoError(t, err)
		t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })

		bundleWaiter := &Waiter{
			Check: func() bool {
				_, err := jobqueue.GetJobBundle(t.Context(), bundleID)
				return err != nil
			},
			Timeout:       5 * time.Second,
			PollFrequency: 50 * time.Millisecond,
		}
		require.NoError(t, bundleWaiter.Wait(), "bundle should complete without CHECK constraint violation")
	})

	t.Run("ResetJob is a no-op for non-bundle jobs", func(t *testing.T) {
		jobType := "f47ac10b-58cc-4372-a567-000000000003"

		jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
			return nil, errors.New("intentional failure")
		})
		t.Cleanup(func() { jobworker.Unregister(jobType) })

		jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000031")
		job, err := jobqueue.NewJob(jobID, jobType, "test-reset", "{}", nullable.Time{})
		require.NoError(t, err)

		err = jobqueue.Add(t.Context(), job)
		require.NoError(t, err)
		t.Cleanup(func() { jobqueue.DeleteJob(context.Background(), jobID) })

		// Process the job (fails)
		err = jobworker.StartThreads(t.Context(), 1)
		require.NoError(t, err)

		waiter := NewJobWaiter(t.Context(), t, jobID)
		require.NoError(t, waiter.Wait())

		jobworker.FinishThreads(context.Background())

		// Reset the non-bundle job — should not error
		err = jobqueue.ResetJob(t.Context(), jobID)
		require.NoError(t, err)

		// Verify the job was reset
		j, err := jobqueue.GetJob(t.Context(), jobID)
		require.NoError(t, err)
		assert.False(t, j.Started())
		assert.False(t, j.Stopped())
		assert.False(t, j.HasError())
	})

	t.Run("ResetJob skips decrement for retryable bundle jobs not yet counted", func(t *testing.T) {
		jobType := "f47ac10b-58cc-4372-a567-000000000004"

		jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
			return nil, errors.New("intentional failure")
		})
		jobworker.RegisterScheduleRetry(jobType, func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
			// Schedule far in the future so the retry doesn't run during the test
			return time.Now().Add(time.Hour), nil
		})
		t.Cleanup(func() { jobworker.Unregister(jobType) })

		bundleID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000004")
		jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-b00000000041")

		job, err := jobqueue.NewJob(jobID, jobType, "test-reset", "{}", nullable.Time{}, 3)
		require.NoError(t, err)

		bundle := &jobqueue.JobBundle{
			ID:      bundleID,
			Type:    "test-reset-bundle",
			Origin:  "test",
			NumJobs: 1,
			Jobs:    []*jobqueue.Job{job},
		}
		err = jobqueue.AddBundle(t.Context(), bundle)
		require.NoError(t, err)

		t.Cleanup(func() {
			bgCtx := context.Background()
			jobqueue.GetService(bgCtx).DeleteJobBundle(bgCtx, bundleID)
			jobqueue.DeleteJob(bgCtx, jobID)
		})

		// Process the job — fails but has retries, so NOT counted in bundle
		err = jobworker.StartThreads(t.Context(), 1)
		require.NoError(t, err)

		// Wait for the job to be retried (scheduled for future)
		retryWaiter := &Waiter{
			Check: func() bool {
				j, err := jobqueue.GetJob(t.Context(), jobID)
				if err != nil {
					return false
				}
				// After ScheduleRetry: started_at is null, current_retry_count > 0
				return !j.Started() && j.CurrentRetryCount > 0
			},
			Timeout:       5 * time.Second,
			PollFrequency: 50 * time.Millisecond,
		}
		require.NoError(t, retryWaiter.Wait())

		jobworker.FinishThreads(context.Background())

		// Bundle counter should NOT have been incremented (job still has retries)
		b, err := jobqueue.GetJobBundle(t.Context(), bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "retryable job should not be counted yet")

		// Reset the job — should not decrement (counter is already 0)
		err = jobqueue.ResetJob(t.Context(), jobID)
		require.NoError(t, err)

		b, err = jobqueue.GetJobBundle(t.Context(), bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "counter should still be 0 after resetting uncounted job")
	})
}

// TestSetJobErrorTerminalForMissingScheduler is a regression test: a retryable
// job whose type has NO registered retry scheduler must fail TERMINALLY rather
// than being left stopped-with-retries-remaining. Before the fix, SetJobError
// left current_retry_count < max_retry_count, which (a) never incremented the
// bundle's num_jobs_stopped counter, so the bundle could never complete, and
// (b) matched the startup reaper's retries-remaining branch, so the job was
// reset and re-run on every startup forever. SetJobError now clamps
// current_retry_count to max_retry_count, making the failure terminal and
// counted.
func TestSetJobErrorTerminalForMissingScheduler(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	jobType := "test-missing-scheduler-terminal-type"

	// Worker always fails; deliberately no RegisterScheduleRetry for jobType, so
	// doJobAndSaveResultInDB takes the "no retry scheduler" branch.
	jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
		return nil, errors.New("intentional failure")
	})
	t.Cleanup(func() { jobworker.Unregister(jobType) })

	bundleID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000031")
	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000032")

	// max_retry_count = 3, so the job starts with retries remaining.
	job, err := jobqueue.NewJob(jobID, jobType, "test-missing-scheduler", "{}", nullable.Time{}, 3)
	require.NoError(t, err)

	bundle := &jobqueue.JobBundle{
		ID:      bundleID,
		Type:    "test-missing-scheduler-bundle",
		Origin:  "test",
		NumJobs: 1,
		Jobs:    []*jobqueue.Job{job},
	}
	require.NoError(t, jobqueue.AddBundle(t.Context(), bundle))
	t.Cleanup(func() {
		bg := context.Background()
		jobqueue.GetService(bg).DeleteJobBundle(bg, bundleID)
		jobqueue.DeleteJob(bg, jobID)
	})

	require.NoError(t, jobworker.StartThreads(t.Context(), 1))
	t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })

	// Wait until the job has stopped with an error.
	stopped := &Waiter{
		Check: func() bool {
			j, err := jobqueue.GetJob(t.Context(), jobID)
			return err == nil && j.Stopped() && j.HasError()
		},
		Timeout:       5 * time.Second,
		PollFrequency: 50 * time.Millisecond,
	}
	require.NoError(t, stopped.Wait())

	j, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.True(t, j.Stopped(), "job should be stopped")
	assert.True(t, j.HasError(), "job should be errored")
	assert.Equal(t, j.MaxRetryCount, j.CurrentRetryCount,
		"SetJobError must clamp current_retry_count to max_retry_count (terminal failure)")
	assert.True(t, j.WorkerAliveAt.IsNull(), "worker_alive_at should be cleared")

	// The bundle must count this terminal failure so the bundle can complete.
	b, err := jobqueue.GetJobBundle(t.Context(), bundleID)
	require.NoError(t, err)
	assert.Equal(t, 1, b.NumJobsStopped, "a terminal failure must be counted in its bundle")
}
