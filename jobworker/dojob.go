package jobworker

import (
	"context"
	"strings"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-types/nullable"
)

// DoJob does a job synchronously and returns the the job result
// or an error without modifying the passed job or saving
// the results in the database.
func DoJob(ctx context.Context, job *jobqueue.Job) (result any, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = errs.Errorf("job worker panic: %w", errs.AsErrorWithDebugStack(p))
		}
		errs.WrapWithFuncParams(&err, job)
	}()

	if job == nil {
		return nil, errs.New("got nil job")
	}

	workersMtx.RLock()
	worker, hasWorker := workers[job.Type]
	workersMtx.RUnlock()

	if !hasWorker {
		return nil, errs.Errorf("no worker for job of type '%s'", job.Type)
	}

	return worker.DoJob(ctx, job)
}

func doJobAndSetResultInDB(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, job)

	result, err := DoJob(ctx, job)
	if err != nil {
		errHeadline := errs.Root(err).Error()
		if nl := strings.IndexByte(errHeadline, '\n'); nl > 0 {
			// Only use first line of error message as errHeadline
			errHeadline = errHeadline[:nl]
		}
		log.Errorf("Job error: %s", errHeadline).
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()

		e := db.SetJobError(ctx, job.ID, err.Error(), nil)
		if e != nil {
			log.Error("Error while updating job error in the database").
				UUID("jobID", job.ID).
				Any("job", job).
				Err(e).
				Log()
			OnError(e)
			return e
		}

		return nil
	}

	resultJSON, err := nullable.MarshalJSON(result)
	if err != nil {
		return err
	}

	return db.SetJobResult(ctx, job.ID, resultJSON)
}
