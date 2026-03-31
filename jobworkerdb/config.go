package jobworkerdb

import (
	"context"

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
// jobs that were interrupted by a previous shutdown or crash.
//
// See InitJobQueueResetDanglingJobs for an alternative that also resets
// jobs that were mid-execution when the process crashed.
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

// InitJobQueueResetInterruptedJobs calls InitJobQueue and additionally
// resets jobs that were interrupted by a previous shutdown or crash.
// These have started_at and stopped_at set (marked as errored)
// but still have retries remaining and should be retried.
func InitJobQueueResetInterruptedJobs(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	err = InitJobQueue(ctx)
	if err != nil {
		return err
	}

	numReset, err := resetInterruptedRetryableJobs(ctx)
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

// InitJobQueueResetDanglingJobs calls InitJobQueueResetInterruptedJobs
// and additionally resets jobs that were mid-execution when the process
// crashed or was killed (started_at set, stopped_at NULL). These jobs
// appear to be running but no worker is processing them.
//
// This is safe when only a single worker instance is running.
// In a multi-instance setup, do NOT use this function because another
// instance may still be actively processing these jobs. Instead, use
// GetAllJobsStartedBefore with a sufficient timeout to identify and
// reset truly abandoned jobs.
func InitJobQueueResetDanglingJobs(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	err = InitJobQueueResetInterruptedJobs(ctx)
	if err != nil {
		return err
	}

	numReset, err := resetDanglingStartedJobs(ctx)
	if err != nil {
		return err
	}
	if numReset > 0 {
		log.Info("Reset dangling started jobs on startup").
			Int("numReset", numReset).
			Log()
	}

	return nil
}

// resetInterruptedRetryableJobs resets jobs that were left in an errored
// state by a previous shutdown or crash but still have retries remaining.
// This clears their execution state so they can be picked up again.
// Returns the number of jobs that were reset.
func resetInterruptedRetryableJobs(ctx context.Context) (numReset int, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	return db.QueryRowAs[int](ctx,
		/*sql*/ `
			with resets as (
				update worker.job
				set
					started_at=null,
					stopped_at=null,
					error_msg =null,
					error_data=null,
					result    =null,
					updated_at=now()
				where started_at is not null
					and stopped_at is not null
					and current_retry_count < max_retry_count
				returning id
			)
			select count(*) from resets
		`,
	)
}

// resetDanglingStartedJobs resets jobs that were mid-execution when the
// process crashed or was killed. These have started_at set but stopped_at
// NULL, meaning a worker claimed them but never finished.
//
// Only use this in single-instance setups. In a multi-instance setup,
// another instance may still be actively processing these jobs.
// Returns the number of jobs that were reset.
func resetDanglingStartedJobs(ctx context.Context) (numReset int, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	return db.QueryRowAs[int](ctx,
		/*sql*/ `
			with resets as (
				update worker.job
				set
					started_at=null,
					stopped_at=null,
					error_msg =null,
					error_data=null,
					result    =null,
					updated_at=now()
				where started_at is not null
					and stopped_at is null
				returning id
			)
			select count(*) from resets
		`,
	)
}
