package jobworker

import (
	"context"
	"strings"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-types/nullable"
)

func dispatchJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, job)

	if job == nil {
		return errs.New("got nil job")
	}

	log, ctx := log.With().
		UUID("jobID", job.ID).
		Any("job", job).
		SubLoggerContext(ctx)

	workersMtx.RLock()
	worker, hasWorker := workers[job.Type]
	workersMtx.RUnlock()

	if !hasWorker {
		return errs.Errorf("no worker for job of type '%s'", job.Type)
	}

	result, err := doJobWithWorker(ctx, worker, job)
	if err != nil {
		errHeadline := errs.Root(err).Error()
		if nl := strings.IndexByte(errHeadline, '\n'); nl > 0 {
			// Only use first line of error message as errHeadline
			errHeadline = errHeadline[:nl]
		}
		log.Errorf("Job error: %s", errHeadline).
			Err(err).
			Log()

		e := db.SetJobError(ctx, job.ID, err.Error(), nil)
		if e != nil {
			log.Error("Error while updating job error in the database").Err(e).Log()
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

func doJobWithWorker(ctx context.Context, worker Worker, job *jobqueue.Job) (result interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errs.Errorf("job worker panic: %w", errs.AsErrorWithDebugStack(r))
		}
	}()

	return worker.DoJob(ctx, job)
}
