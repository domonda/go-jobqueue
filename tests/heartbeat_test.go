package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
)

func TestJobHeartbeat(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	// Speed up the heartbeat so the test doesn't have to wait 10 seconds.
	origInterval := jobworker.HeartbeatInterval
	jobworker.HeartbeatInterval = 50 * time.Millisecond
	t.Cleanup(func() { jobworker.HeartbeatInterval = origInterval })

	jobType := "test-heartbeat-job-type"

	// The worker blocks until released (or ctx is cancelled) so the test can
	// observe the heartbeat advancing worker_alive_at while the job is processing.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }

	jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return "ok", nil
	})
	t.Cleanup(func() { jobworker.Unregister(jobType) })

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000011")
	job, err := jobqueue.NewJob(jobID, jobType, "test-heartbeat", "{}", nullable.TimeNow())
	require.NoError(t, err)
	err = jobqueue.Add(t.Context(), job)
	require.NoError(t, err)
	t.Cleanup(func() { jobqueue.DeleteJob(context.Background(), jobID) })

	err = jobworker.StartThreads(t.Context(), 1)
	require.NoError(t, err)
	// FinishThreads is registered before releaseWorker so that releaseWorker
	// (and thus the worker returning) runs first during cleanup, preventing
	// FinishThreads from blocking on a still-running worker.
	t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })
	t.Cleanup(releaseWorker)

	// Wait until the worker has claimed the job and is processing it.
	processingWaiter := &Waiter{
		Check: func() bool {
			j, err := jobqueue.GetJob(t.Context(), jobID)
			require.NoError(t, err)
			return j.StartedAndNotStopped()
		},
		Timeout:       2 * time.Second,
		PollFrequency: 20 * time.Millisecond,
	}
	require.NoError(t, processingWaiter.Wait())

	j, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	require.True(t, j.WorkerAliveAt.IsNotNull(), "worker_alive_at should be set when the job is claimed")
	firstAlive := j.WorkerAliveAt.Get()

	// The heartbeat goroutine must keep advancing worker_alive_at while processing.
	advanceWaiter := &Waiter{
		Check: func() bool {
			j, err := jobqueue.GetJob(t.Context(), jobID)
			require.NoError(t, err)
			return j.WorkerAliveAt.IsNotNull() && j.WorkerAliveAt.Get().After(firstAlive)
		},
		Timeout:       2 * time.Second,
		PollFrequency: 20 * time.Millisecond,
	}
	require.NoError(t, advanceWaiter.Wait(), "worker_alive_at should advance via heartbeat while processing")

	j, err = jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.True(t, j.WorkerAlive(time.Second), "worker should be considered alive with a fresh heartbeat")

	// Let the worker finish.
	releaseWorker()

	finishWaiter := NewJobWaiter(t.Context(), t, jobID)
	finishWaiter.Timeout = 3 * time.Second
	require.NoError(t, finishWaiter.Wait())

	// After completion worker_alive_at must be cleared and the job stopped without error.
	j, err = jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.True(t, j.Stopped(), "job should be stopped after completion")
	assert.False(t, j.HasError(), "job should not have an error")
	assert.True(t, j.WorkerAliveAt.IsNull(), "worker_alive_at should be cleared after completion")
	assert.False(t, j.WorkerAlive(time.Hour), "a stopped job is not being processed")
}

// TestJobHeartbeatDisabled verifies that with heartbeats disabled
// (HeartbeatInterval <= 0) a claimed job's worker_alive_at stays NULL, so the
// heartbeat-staleness reaper branch can never treat it as a crashed worker and
// reset a still-running job (which would cause double execution).
func TestJobHeartbeatDisabled(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	origInterval := jobworker.HeartbeatInterval
	jobworker.HeartbeatInterval = 0 // disable heartbeats
	t.Cleanup(func() { jobworker.HeartbeatInterval = origInterval })

	jobType := "test-heartbeat-disabled-job-type"

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }

	jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return "ok", nil
	})
	t.Cleanup(func() { jobworker.Unregister(jobType) })

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000012")
	job, err := jobqueue.NewJob(jobID, jobType, "test-heartbeat-disabled", "{}", nullable.TimeNow())
	require.NoError(t, err)
	err = jobqueue.Add(t.Context(), job)
	require.NoError(t, err)
	t.Cleanup(func() { jobqueue.DeleteJob(context.Background(), jobID) })

	err = jobworker.StartThreads(t.Context(), 1)
	require.NoError(t, err)
	t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })
	t.Cleanup(releaseWorker)

	processingWaiter := &Waiter{
		Check: func() bool {
			j, err := jobqueue.GetJob(t.Context(), jobID)
			require.NoError(t, err)
			return j.StartedAndNotStopped()
		},
		Timeout:       2 * time.Second,
		PollFrequency: 20 * time.Millisecond,
	}
	require.NoError(t, processingWaiter.Wait())

	j, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	assert.True(t, j.WorkerAliveAt.IsNull(), "worker_alive_at must stay NULL when heartbeats are disabled")
	assert.False(t, j.WorkerAlive(time.Hour), "a job without a heartbeat is never reported alive")
}

// TestRetrySchedulerRunsUnderLiveHeartbeat is a regression test for the
// multi-process safety of the error→retry path. A retryable job must stay
// "claimed and live" (started, not stopped, heartbeat advancing) for the whole
// time its retry scheduler runs, so another process's reaper cannot reclaim a
// job this worker still owns even when the scheduler does slow work. Before the
// fix the job was marked stopped+errored before the scheduler ran, exposing it
// to the reaper's error→retry branch for the scheduler's entire duration.
func TestRetrySchedulerRunsUnderLiveHeartbeat(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	// Fast heartbeat so the test can observe worker_alive_at advancing quickly.
	origInterval := jobworker.HeartbeatInterval
	jobworker.HeartbeatInterval = 50 * time.Millisecond
	t.Cleanup(func() { jobworker.HeartbeatInterval = origInterval })

	jobType := "test-retry-live-heartbeat-type"

	// The retry scheduler blocks until released, standing in for slow work (a
	// network call, a backoff lookup, …) between job failure and reschedule.
	schedulerEntered := make(chan struct{})
	releaseScheduler := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseScheduler) }) }

	jobworker.Register(jobType, func(ctx context.Context, job *jobqueue.Job) (any, error) {
		return nil, errors.New("intentional failure to trigger a retry")
	})
	jobworker.RegisterScheduleRetry(jobType, func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
		enteredOnce.Do(func() { close(schedulerEntered) })
		select {
		case <-releaseScheduler:
		case <-ctx.Done():
		}
		return time.Now().Add(time.Hour), nil
	})
	t.Cleanup(func() { jobworker.Unregister(jobType) })

	jobID := uu.IDFrom("f47ac10b-58cc-4372-a567-e00000000021")
	job, err := jobqueue.NewJob(jobID, jobType, "test-retry-live-heartbeat", "{}", nullable.Time{}, 3)
	require.NoError(t, err)
	require.NoError(t, jobqueue.Add(t.Context(), job))
	t.Cleanup(func() { jobqueue.DeleteJob(context.Background(), jobID) })

	require.NoError(t, jobworker.StartThreads(t.Context(), 1))
	// FinishThreads is registered before release so that release (LIFO cleanup)
	// runs first, letting the worker return before FinishThreads waits on it.
	t.Cleanup(func() { jobworker.FinishThreads(context.Background()) })
	t.Cleanup(release)

	// Wait until the worker has failed the job and entered the blocking scheduler.
	select {
	case <-schedulerEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("retry scheduler was not entered in time")
	}

	// Core invariant: while the scheduler runs, the job stays started+not-stopped
	// with a non-NULL heartbeat and is NOT yet marked errored. On the old code it
	// was already stopped+errored here, so all three of these would fail.
	j, err := jobqueue.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	require.True(t, j.StartedAndNotStopped(), "job must stay started+not-stopped while the retry scheduler runs")
	require.True(t, j.WorkerAliveAt.IsNotNull(), "job must carry a heartbeat while the retry scheduler runs")
	require.False(t, j.HasError(), "job must not be marked errored before the retry is scheduled")
	first := j.WorkerAliveAt.Get()

	// The heartbeat must keep advancing during the blocked scheduler call.
	advance := &Waiter{
		Check: func() bool {
			j, err := jobqueue.GetJob(t.Context(), jobID)
			require.NoError(t, err)
			return j.StartedAndNotStopped() && j.WorkerAliveAt.IsNotNull() && j.WorkerAliveAt.Get().After(first)
		},
		Timeout:       2 * time.Second,
		PollFrequency: 20 * time.Millisecond,
	}
	require.NoError(t, advance.Wait(), "worker_alive_at must keep advancing while the retry scheduler runs")

	// Release the scheduler; the job must transition to scheduled-for-retry with
	// its heartbeat cleared and one retry consumed.
	release()

	retried := &Waiter{
		Check: func() bool {
			j, err := jobqueue.GetJob(t.Context(), jobID)
			require.NoError(t, err)
			return !j.Started() && j.CurrentRetryCount == 1 && j.WorkerAliveAt.IsNull()
		},
		Timeout:       3 * time.Second,
		PollFrequency: 20 * time.Millisecond,
	}
	require.NoError(t, retried.Wait(), "job should be scheduled for retry after the scheduler returns")
}
