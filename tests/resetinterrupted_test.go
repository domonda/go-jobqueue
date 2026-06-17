package tests

import (
	"context"
	"testing"
	"time"

	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/uu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
	"github.com/domonda/go-jobqueue/jobworkerdb"
)

// TestResetInterruptedRetryableJobs verifies that InitJobQueueResetInterruptedJobs
// only reclaims jobs whose worker is provably dead (multi-process safe) and that
// a crashed final attempt is reclaimed regardless of the retry count.
func TestResetInterruptedRetryableJobs(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	_ = jobqueue.Close()
	setupDBConn(t)
	ctx := t.Context()

	const origin = "test-reset-interrupted"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin = $1`, origin)
		_ = jobqueue.Close()
	})

	now := time.Now()
	stale := now.Add(-5 * time.Minute) // older than deadFor → worker presumed dead
	fresh := now.Add(-1 * time.Second) // within deadFor → worker alive
	deadFor := time.Minute

	// insertJob inserts a worker.job row in a precise state. Pass nil for a
	// NULL timestamp or error message.
	insertJob := func(t *testing.T, id uu.ID, maxRetry, curRetry int, startedAt, stoppedAt, workerAliveAt, errorMsg any) {
		t.Helper()
		err := db.Exec(ctx,
			/*sql*/ `
				insert into worker.job (
					id, type, payload, priority, origin,
					max_retry_count, current_retry_count,
					started_at, stopped_at, worker_alive_at, error_msg
				) values ($1, $2, '{}'::jsonb, 0, $3, $4, $5, $6, $7, $8, $9)
			`,
			id, "test-reset-interrupted-type", origin,
			maxRetry, curRetry,
			startedAt, stoppedAt, workerAliveAt, errorMsg,
		)
		require.NoError(t, err)
	}

	id := func(suffix string) uu.ID {
		return uu.IDFrom("f47ac10b-58cc-4372-a567-c000000000" + suffix)
	}

	var (
		liveInProgress          = id("01") // running, fresh heartbeat             -> keep
		crashedInProgress       = id("02") // running, stale heartbeat             -> reset
		crashedFinalAttempt     = id("03") // running, stale, retries exhausted   -> reset (rule #2)
		erroredStuck            = id("04") // errored long ago, retries remaining  -> reset
		erroredTransient        = id("05") // errored just now, retries remaining  -> keep
		permanentlyFailed       = id("06") // errored, retries exhausted           -> keep
		succeeded               = id("07") // finished without error               -> keep
		nullHeartbeatInProgress = id("08") // running, no heartbeat at all        -> keep
	)

	insertJob(t, liveInProgress, 3, 0, stale, nil, fresh, nil)
	insertJob(t, crashedInProgress, 3, 0, stale, nil, stale, nil)
	insertJob(t, crashedFinalAttempt, 3, 3, stale, nil, stale, nil)
	insertJob(t, erroredStuck, 3, 1, stale, stale, nil, "boom")
	insertJob(t, erroredTransient, 3, 1, stale, now, nil, "boom")
	insertJob(t, permanentlyFailed, 3, 3, stale, stale, nil, "boom")
	insertJob(t, succeeded, 3, 0, stale, stale, nil, nil)
	// started long ago, never stopped, but worker_alive_at is NULL: either claimed
	// before the heartbeat column existed (pre-deploy) or with heartbeats disabled.
	// Without a heartbeat there is no liveness signal, so it must NOT be reclaimed
	// here (it would otherwise be a candidate for double execution).
	insertJob(t, nullHeartbeatInProgress, 3, 0, stale, nil, nil, nil)

	// Re-init and run the reaper. Close first so the new service can re-listen.
	_ = jobqueue.Close()
	require.NoError(t, jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, deadFor))

	get := func(t *testing.T, jobID uu.ID) *jobqueue.Job {
		t.Helper()
		j, err := jobqueue.GetJob(ctx, jobID)
		require.NoError(t, err)
		return j
	}

	// assertReset verifies the job was cleared back to a claimable state.
	assertReset := func(t *testing.T, jobID uu.ID) {
		t.Helper()
		j := get(t, jobID)
		assert.False(t, j.Started(), "started_at should be cleared")
		assert.False(t, j.Stopped(), "stopped_at should be cleared")
		assert.False(t, j.HasError(), "error_msg should be cleared")
		assert.True(t, j.WorkerAliveAt.IsNull(), "worker_alive_at should be cleared")
	}

	t.Run("crashed in-progress job is reset", func(t *testing.T) {
		assertReset(t, crashedInProgress)
	})

	t.Run("crashed final attempt is reset and keeps its retry count", func(t *testing.T) {
		assertReset(t, crashedFinalAttempt)
		// The count is unchanged so the interrupted final attempt runs once more;
		// a crash is not a consumed attempt.
		assert.Equal(t, 3, get(t, crashedFinalAttempt).CurrentRetryCount)
	})

	t.Run("errored job stuck before ScheduleRetry is reset", func(t *testing.T) {
		assertReset(t, erroredStuck)
	})

	t.Run("live in-progress job with fresh heartbeat is kept", func(t *testing.T) {
		j := get(t, liveInProgress)
		assert.True(t, j.StartedAndNotStopped(), "a job with a live worker must not be rug-pulled")
	})

	t.Run("recently errored job mid-transition is kept", func(t *testing.T) {
		j := get(t, erroredTransient)
		assert.True(t, j.Started() && j.Stopped() && j.HasError(),
			"a job a live worker is about to retry must not be reset")
	})

	t.Run("permanently failed job is kept", func(t *testing.T) {
		j := get(t, permanentlyFailed)
		assert.True(t, j.HasError() && j.Stopped(), "a genuine final failure must stay failed")
	})

	t.Run("succeeded job is kept", func(t *testing.T) {
		j := get(t, succeeded)
		assert.True(t, j.Stopped() && !j.HasError(), "a succeeded job must not be reset")
	})

	t.Run("in-progress job without a heartbeat is kept", func(t *testing.T) {
		j := get(t, nullHeartbeatInProgress)
		assert.True(t, j.StartedAndNotStopped(), "a started job with NULL worker_alive_at has no liveness signal and must not be reset")
		assert.True(t, j.WorkerAliveAt.IsNull())
	})
}

// TestInitJobQueueResetInterruptedJobsValidatesDeadFor verifies that a deadFor
// that would rug-pull actively running jobs is rejected loudly instead of
// silently corrupting state.
func TestInitJobQueueResetInterruptedJobsValidatesDeadFor(t *testing.T) {
	ctx := t.Context()

	t.Run("non-positive deadFor is rejected", func(t *testing.T) {
		assert.Error(t, jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, 0))
		assert.Error(t, jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, -time.Second))
	})

	t.Run("deadFor not larger than HeartbeatInterval is rejected", func(t *testing.T) {
		// Default HeartbeatInterval is 10s; a 5s deadFor would reset live jobs.
		require.Positive(t, jobworker.HeartbeatInterval)
		assert.Error(t, jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, jobworker.HeartbeatInterval))
		assert.Error(t, jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, jobworker.HeartbeatInterval/2))
	})
}
