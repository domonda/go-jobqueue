package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
)

func TestResetJobDecrementsBundleCounter(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	// pqconn uses a global listener per connection URL, so calling
	// ListenOnChannel again without unlistening first fails with
	// "channel is already open".
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

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
		err = jobqueue.AddBundle(ctx, bundle)
		require.NoError(t, err)

		t.Cleanup(func() {
			bgCtx := context.Background()
			// Bundle may be auto-deleted after success; ignore errors
			jobqueue.GetService(bgCtx).DeleteJobBundle(bgCtx, bundleID)
			jobqueue.DeleteJob(bgCtx, job1ID)
			jobqueue.DeleteJob(bgCtx, job2ID)
		})

		// Phase 1: Process jobs — both fail (retryCount=0, so they're final failures)
		err = jobworker.StartThreads(ctx, 1)
		require.NoError(t, err)

		waiter1 := NewJobWaiter(ctx, t, job1ID)
		waiter2 := NewJobWaiter(ctx, t, job2ID)
		require.NoError(t, waiter1.Wait())
		require.NoError(t, waiter2.Wait())

		jobworker.FinishThreads(context.Background())

		// Verify both jobs failed
		j1, err := jobqueue.GetJob(ctx, job1ID)
		require.NoError(t, err)
		require.True(t, j1.HasError(), "job1 should have error")

		j2, err := jobqueue.GetJob(ctx, job2ID)
		require.NoError(t, err)
		require.True(t, j2.HasError(), "job2 should have error")

		// Verify bundle counter was incremented for both final failures
		b, err := jobqueue.GetJobBundle(ctx, bundleID)
		require.NoError(t, err)
		assert.Equal(t, 2, b.NumJobsStopped, "both failed jobs should be counted in bundle")

		// Phase 2: Reset both jobs — counter must be decremented
		err = jobqueue.ResetJob(ctx, job1ID)
		require.NoError(t, err)
		err = jobqueue.ResetJob(ctx, job2ID)
		require.NoError(t, err)

		b, err = jobqueue.GetJobBundle(ctx, bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "counter should be decremented after ResetJob")

		// Verify jobs were actually reset
		j1, err = jobqueue.GetJob(ctx, job1ID)
		require.NoError(t, err)
		assert.False(t, j1.Started(), "job1 should not be started after reset")
		assert.False(t, j1.Stopped(), "job1 should not be stopped after reset")
		assert.False(t, j1.HasError(), "job1 should not have error after reset")

		// Phase 3: Process again — jobs succeed this time
		// Without the fix, SetJobResult would try to increment num_jobs_stopped
		// past num_jobs, violating the CHECK constraint.
		shouldSucceed.Store(true)

		err = jobworker.StartThreads(ctx, 1)
		require.NoError(t, err)
		t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })

		// Wait for the bundle to complete (auto-deleted on success)
		bundleWaiter := &Waiter{
			Check: func() bool {
				_, err := jobqueue.GetJobBundle(ctx, bundleID)
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
		err = jobqueue.AddBundle(ctx, bundle)
		require.NoError(t, err)

		t.Cleanup(func() {
			bgCtx := context.Background()
			jobqueue.GetService(bgCtx).DeleteJobBundle(bgCtx, bundleID)
			jobqueue.DeleteJob(bgCtx, job1ID)
			jobqueue.DeleteJob(bgCtx, job2ID)
		})

		// Phase 1: Both jobs fail
		err = jobworker.StartThreads(ctx, 1)
		require.NoError(t, err)

		waiter1 := NewJobWaiter(ctx, t, job1ID)
		waiter2 := NewJobWaiter(ctx, t, job2ID)
		require.NoError(t, waiter1.Wait())
		require.NoError(t, waiter2.Wait())

		jobworker.FinishThreads(context.Background())

		b, err := jobqueue.GetJobBundle(ctx, bundleID)
		require.NoError(t, err)
		require.Equal(t, 2, b.NumJobsStopped, "both failed jobs should be counted")

		// Phase 2: Reset both jobs in one call
		err = jobqueue.ResetJobs(ctx, uu.IDSlice{job1ID, job2ID})
		require.NoError(t, err)

		b, err = jobqueue.GetJobBundle(ctx, bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "counter should be decremented after ResetJobs")

		// Phase 3: Jobs succeed
		shouldSucceed.Store(true)

		err = jobworker.StartThreads(ctx, 1)
		require.NoError(t, err)
		t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })

		bundleWaiter := &Waiter{
			Check: func() bool {
				_, err := jobqueue.GetJobBundle(ctx, bundleID)
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

		err = jobqueue.Add(ctx, job)
		require.NoError(t, err)
		t.Cleanup(func() { jobqueue.DeleteJob(context.Background(), jobID) })

		// Process the job (fails)
		err = jobworker.StartThreads(ctx, 1)
		require.NoError(t, err)

		waiter := NewJobWaiter(ctx, t, jobID)
		require.NoError(t, waiter.Wait())

		jobworker.FinishThreads(context.Background())

		// Reset the non-bundle job — should not error
		err = jobqueue.ResetJob(ctx, jobID)
		require.NoError(t, err)

		// Verify the job was reset
		j, err := jobqueue.GetJob(ctx, jobID)
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
		err = jobqueue.AddBundle(ctx, bundle)
		require.NoError(t, err)

		t.Cleanup(func() {
			bgCtx := context.Background()
			jobqueue.GetService(bgCtx).DeleteJobBundle(bgCtx, bundleID)
			jobqueue.DeleteJob(bgCtx, jobID)
		})

		// Process the job — fails but has retries, so NOT counted in bundle
		err = jobworker.StartThreads(ctx, 1)
		require.NoError(t, err)

		// Wait for the job to be retried (scheduled for future)
		retryWaiter := &Waiter{
			Check: func() bool {
				j, err := jobqueue.GetJob(ctx, jobID)
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
		b, err := jobqueue.GetJobBundle(ctx, bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "retryable job should not be counted yet")

		// Reset the job — should not decrement (counter is already 0)
		err = jobqueue.ResetJob(ctx, jobID)
		require.NoError(t, err)

		b, err = jobqueue.GetJobBundle(ctx, bundleID)
		require.NoError(t, err)
		assert.Equal(t, 0, b.NumJobsStopped, "counter should still be 0 after resetting uncounted job")
	})
}
