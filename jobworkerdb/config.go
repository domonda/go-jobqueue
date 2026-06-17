package jobworkerdb

import (
	"context"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
	"github.com/domonda/go-sqldb/db"
	rootlog "github.com/domonda/golog/log"
)

var log = rootlog.NewPackageLogger()

// InitJobQueue initializes the job queue with a PostgreSQL-backed service
// without resetting any jobs.
//
// See InitJobQueueResetInterruptedJobs for an alternative that also resets
// jobs that were abandoned by a crashed worker.
//
// Jobs that were mid-execution when the process crashed (started_at set,
// stopped_at NULL) are no longer reset blindly on startup. Use the
// worker_alive_at heartbeat instead to detect abandoned jobs: a job with a
// stale worker_alive_at was abandoned by a crashed worker
// (see [jobqueue.Job.WorkerAlive]).
func InitJobQueue(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	jwDB := &jobworkerDB{}

	err = jwDB.AddListener(ctx, jobqueue.NewDefaultServiceListener(jwDB))
	if err != nil {
		return err
	}

	jobworker.SetDataBase(jwDB)

	jobqueue.SetDefaultService(jwDB)

	return nil
}

// minDeadForHeartbeatFactor is the minimum multiple of
// jobworker.HeartbeatInterval that deadFor must reach (when heartbeats are
// enabled) before a job is treated as abandoned. The factor leaves room for one
// fully missed or delayed heartbeat write, so a live worker is never reset;
// deadFor == HeartbeatInterval (or a hair above) would reap a worker that merely
// skipped a single heartbeat.
const minDeadForHeartbeatFactor = 2

// InitJobQueueResetInterruptedJobs calls InitJobQueue and additionally resets
// retryable jobs that were abandoned by a worker that crashed at least deadFor
// ago, so they can be picked up again.
//
// deadFor is the grace period after which a worker is presumed dead. It must be
// comfortably larger than [jobworker.HeartbeatInterval] (and any retry-scheduler
// delay) so that jobs being actively processed or transitioned by a live worker
// in another process are never reset. This makes the reset safe to run on
// startup even when multiple worker processes share the same queue.
//
// It returns an error if deadFor is not positive, or — when heartbeats are
// enabled — if deadFor is not at least minDeadForHeartbeatFactor ×
// [jobworker.HeartbeatInterval]. A too-small deadFor would rug-pull actively
// running jobs, so the misconfiguration fails loudly instead of silently
// corrupting state.
func InitJobQueueResetInterruptedJobs(ctx context.Context, deadFor time.Duration) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, deadFor)

	if deadFor <= 0 {
		return errs.Errorf("deadFor must be positive, got %s", deadFor)
	}
	// The heartbeat-staleness branch of the reaper presumes a worker dead once its
	// worker_alive_at is older than deadFor. A live worker refreshes worker_alive_at
	// every HeartbeatInterval, so deadFor must span several intervals: requiring a
	// margin (not just deadFor > HeartbeatInterval) absorbs a missed or delayed
	// heartbeat write — caused by GC pauses, DB latency or scheduler jitter — so a
	// live worker is never reset out from under itself.
	//
	// When heartbeats are disabled (HeartbeatInterval <= 0) worker_alive_at stays
	// NULL at claim time and the heartbeat-staleness branch is inert, so this margin
	// does not apply and any positive deadFor is accepted. The error→retry branch
	// still runs, but it no longer races a live worker: the retry scheduler runs
	// while the job is still claimed (see doJobAndSaveResultInDB) and the job is only
	// marked errored once the worker has returned, so the branch reclaims a job only
	// after its worker is gone, independent of scheduler runtime. (A pre-heartbeat
	// worker during a rolling upgrade can still leave that window, so keep deadFor
	// above the slowest scheduler until every process runs this version.)
	if jobworker.HeartbeatInterval > 0 && deadFor < minDeadForHeartbeatFactor*jobworker.HeartbeatInterval {
		return errs.Errorf("deadFor (%s) must be at least %d×jobworker.HeartbeatInterval (%s)", deadFor, minDeadForHeartbeatFactor, jobworker.HeartbeatInterval)
	}

	err = InitJobQueue(ctx)
	if err != nil {
		return err
	}

	numReset, err := resetInterruptedRetryableJobs(ctx, deadFor)
	if err != nil {
		return err
	}
	if numReset > 0 {
		log.Info("Reset interrupted retryable jobs on startup").
			Int("numReset", numReset).
			Log()
	}

	return nil
}

// resetInterruptedRetryableJobs resets retryable jobs that were abandoned by a
// worker that has been gone for at least deadFor, clearing their execution state
// so they can be picked up again. Returns the number of jobs that were reset.
//
// Only jobs whose worker is provably dead are reset, which makes this safe with
// multiple worker processes: a live worker keeps worker_alive_at fresh, and it
// stays claimed (heartbeat advancing, not yet stopped) throughout its retry
// scheduling, so it never crosses the deadFor window. Two abandonment cases are
// covered:
//
//   - Crashed mid-execution: started but not stopped (stopped_at IS NULL) and
//     worker_alive_at has not advanced for at least deadFor. Reset regardless of
//     the retry count so that a crashed final attempt is retried — a crash is
//     not a consumed attempt, and the unchanged retry count gives it exactly one
//     more run of that attempt. Requires worker_alive_at IS NOT NULL, so jobs
//     claimed before this column existed (or while heartbeats were disabled) are
//     not reclaimed here until their worker_alive_at is backfilled to started_at
//     (out-of-band, as part of the migration that adds the column), which marks
//     them as started-but-stale so this branch reclaims them.
//   - Errored with retries remaining but never rescheduled: marked errored
//     (stopped_at and error_msg set) while current_retry_count < max_retry_count.
//     The owning worker is already gone — the retry scheduler runs while the job
//     is still claimed (see doJobAndSaveResultInDB), so a job reaches this state
//     only after its worker returns (a missing or failing retry scheduler), or it
//     was left here by a pre-heartbeat worker during a rolling upgrade. stopped_at
//     records when it last ran; if that was at least deadFor ago the worker is
//     gone. Genuine final failures (current_retry_count >= max_retry_count) are
//     left untouched.
//
// The deadFor cutoff is evaluated entirely with the database clock
// (now() - interval) rather than the app-server clock, so the comparison against
// the DB-written worker_alive_at / stopped_at columns is not affected by clock
// skew between worker processes.
func resetInterruptedRetryableJobs(ctx context.Context, deadFor time.Duration) (numReset int, err error) {
	defer errs.WrapWithFuncParams(&err, ctx, deadFor)

	return db.QueryRowAs[int](ctx,
		/*sql*/ `
			with resets as (
				update worker.job
				set
					started_at     =null,
					stopped_at     =null,
					error_msg      =null,
					error_data     =null,
					result         =null,
					worker_alive_at=null,
					updated_at     =now()
				where started_at is not null
					and (
						(
							-- Crashed mid-execution: heartbeat went stale.
							stopped_at is null
							and worker_alive_at is not null
							and worker_alive_at < now() - make_interval(secs => $1)
						)
						or
						(
							-- Crashed between SetJobError and ScheduleRetry.
							stopped_at is not null
							and error_msg is not null
							and current_retry_count < max_retry_count
							and stopped_at < now() - make_interval(secs => $1)
						)
					)
				returning id
			)
			select count(*) from resets
		`,
		deadFor.Seconds(), // $1
	)
}
