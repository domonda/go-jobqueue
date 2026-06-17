package tests

import (
	"context"
	"testing"
	"time"

	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
)

// dataBaseAPI returns the running service as a jobworker.DataBase so a test can
// drive the low-level SetJobWorkerAlive / SetJobError / ScheduleRetry /
// SetJobStart transitions directly, without registering a worker and racing the
// thread pool. InitJobQueue registers the same *jobworkerDB value as both the
// jobqueue default service and the jobworker DataBase, so the assertion holds.
func dataBaseAPI(t *testing.T, ctx context.Context) jobworker.DataBase {
	t.Helper()
	dbAPI, ok := jobqueue.GetService(ctx).(jobworker.DataBase)
	require.True(t, ok, "default service must implement jobworker.DataBase")
	return dbAPI
}

// TestSetJobWorkerAliveGuard verifies the `started_at is not null and stopped_at
// is null` guard in SetJobWorkerAlive: a heartbeat only advances worker_alive_at
// while the job is actually being processed. The guard prevents a late heartbeat
// that races with completion from resurrecting worker_alive_at on a stopped job,
// which would make the reaper see a stale-but-non-NULL heartbeat and wrongly
// reclaim a finished job.
func TestSetJobWorkerAliveGuard(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

	dbAPI := dataBaseAPI(t, ctx)

	const origin = "test-set-worker-alive-guard"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin = $1`, origin)
	})

	// insertJob inserts a worker.job row with worker_alive_at NULL. Pass nil for
	// a NULL started_at/stopped_at.
	insertJob := func(t *testing.T, id uu.ID, startedAt, stoppedAt any) {
		t.Helper()
		err := db.Exec(ctx,
			/*sql*/ `
				insert into worker.job (
					id, type, payload, priority, origin,
					max_retry_count, current_retry_count,
					started_at, stopped_at, worker_alive_at
				) values ($1, $2, '{}'::jsonb, 0, $3, 0, 0, $4, $5, null)
			`,
			id, "test-set-worker-alive-guard-type", origin, startedAt, stoppedAt,
		)
		require.NoError(t, err)
	}

	get := func(t *testing.T, id uu.ID) *jobqueue.Job {
		t.Helper()
		j, err := jobqueue.GetJob(ctx, id)
		require.NoError(t, err)
		return j
	}

	t.Run("advances worker_alive_at for a job being processed", func(t *testing.T) {
		id := uu.IDFrom("f47ac10b-58cc-4372-a567-d00000000001")
		insertJob(t, id, time.Now(), nil) // started, not stopped
		require.NoError(t, dbAPI.SetJobWorkerAlive(ctx, id))
		assert.True(t, get(t, id).WorkerAliveAt.IsNotNull(),
			"heartbeat must set worker_alive_at on a started, not-stopped job")
	})

	t.Run("is a no-op for a stopped job", func(t *testing.T) {
		id := uu.IDFrom("f47ac10b-58cc-4372-a567-d00000000002")
		insertJob(t, id, time.Now(), time.Now()) // started AND stopped
		require.NoError(t, dbAPI.SetJobWorkerAlive(ctx, id))
		assert.True(t, get(t, id).WorkerAliveAt.IsNull(),
			"guard must not resurrect worker_alive_at on a stopped job")
	})

	t.Run("is a no-op for an unclaimed job", func(t *testing.T) {
		id := uu.IDFrom("f47ac10b-58cc-4372-a567-d00000000003")
		insertJob(t, id, nil, nil) // never started
		require.NoError(t, dbAPI.SetJobWorkerAlive(ctx, id))
		assert.True(t, get(t, id).WorkerAliveAt.IsNull(),
			"guard must not set worker_alive_at on an unclaimed job")
	})
}

// TestWorkerAliveClearedOnTransitions verifies that every transition which stops,
// reschedules, or restarts a job clears worker_alive_at. The reaper's
// heartbeat-staleness branch keys off a non-NULL worker_alive_at, so a leftover
// heartbeat from a previous run would let it wrongly treat the job as a crashed
// worker. Only SetJobResult's clear is covered by TestJobHeartbeat; this covers
// the SetJobError, ScheduleRetry and SetJobStart paths.
func TestWorkerAliveClearedOnTransitions(t *testing.T) {
	// Close any previous service to unlisten channels before re-initializing.
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

	dbAPI := dataBaseAPI(t, ctx)

	const origin = "test-worker-alive-cleared"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin = $1`, origin)
	})

	// insertProcessingJob inserts a started, not-stopped job WITH a non-NULL
	// worker_alive_at — the state a stop/reschedule/restart transition must clear.
	insertProcessingJob := func(t *testing.T, id uu.ID, maxRetry, curRetry int) {
		t.Helper()
		now := time.Now()
		err := db.Exec(ctx,
			/*sql*/ `
				insert into worker.job (
					id, type, payload, priority, origin,
					max_retry_count, current_retry_count,
					started_at, stopped_at, worker_alive_at
				) values ($1, $2, '{}'::jsonb, 0, $3, $4, $5, $6, null, $6)
			`,
			id, "test-worker-alive-cleared-type", origin, maxRetry, curRetry, now,
		)
		require.NoError(t, err)
		// Precondition: the row really does carry a heartbeat to be cleared,
		// otherwise the assertions below would pass vacuously.
		j, err := jobqueue.GetJob(ctx, id)
		require.NoError(t, err)
		require.True(t, j.WorkerAliveAt.IsNotNull(), "fixture must start with a non-NULL worker_alive_at")
	}

	assertCleared := func(t *testing.T, id uu.ID) {
		t.Helper()
		j, err := jobqueue.GetJob(ctx, id)
		require.NoError(t, err)
		assert.True(t, j.WorkerAliveAt.IsNull(), "worker_alive_at must be cleared by the transition")
	}

	t.Run("SetJobError clears worker_alive_at", func(t *testing.T) {
		id := uu.IDFrom("f47ac10b-58cc-4372-a567-d00000000011")
		insertProcessingJob(t, id, 3, 3) // retries exhausted -> final failure
		var errData nullable.JSON        // NULL error_data
		require.NoError(t, dbAPI.SetJobError(ctx, id, "boom", errData))
		assertCleared(t, id)
	})

	t.Run("ScheduleRetry clears worker_alive_at", func(t *testing.T) {
		id := uu.IDFrom("f47ac10b-58cc-4372-a567-d00000000012")
		insertProcessingJob(t, id, 3, 0)
		require.NoError(t, dbAPI.ScheduleRetry(ctx, id, time.Now().Add(time.Hour), 1))
		assertCleared(t, id)
	})

	t.Run("SetJobStart clears worker_alive_at", func(t *testing.T) {
		id := uu.IDFrom("f47ac10b-58cc-4372-a567-d00000000013")
		insertProcessingJob(t, id, 3, 0)
		require.NoError(t, dbAPI.SetJobStart(ctx, id, time.Now().Add(time.Hour)))
		assertCleared(t, id)
	})
}
