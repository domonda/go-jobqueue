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

func InitJobQueue(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	jwDB := &jobworkerDB{}

	err = jwDB.AddListener(ctx, jobqueue.NewDefaultServiceListener(jwDB))
	if err != nil {
		return err
	}

	jobworker.SetDataBase(jwDB)

	jobqueue.SetDefaultService(jwDB)

	// Reset jobs that were interrupted by a previous shutdown or crash.
	// These have started_at and stopped_at set (marked as errored)
	// but still have retries remaining and should be retried.
	numReset, err := resetInterruptedRetryableJobs(ctx)
	if err != nil {
		return err
	}
	if numReset > 0 {
		log.Info("Reset interrupted retryable jobs on startup").
			Int64("numReset", numReset).
			Log()
	}

	return nil
}

// resetInterruptedRetryableJobs resets jobs that were left in an errored
// state by a previous shutdown or crash but still have retries remaining.
// This clears their execution state so they can be picked up again.
// Returns the number of jobs that were reset.
func resetInterruptedRetryableJobs(ctx context.Context) (numReset int64, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	numReset, err = db.QueryValue[int64](ctx,
		/*sql*/ `
			with reset as (
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
			select count(*) from reset
		`,
	)
	return numReset, err
}
