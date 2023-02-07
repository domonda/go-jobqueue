package jobworker

import (
	"context"
	"errors"
	"strings"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-types/nullable"
)

// DoJob does a job synchronously and sets the job.Result
// if there was no error or sets job.ErrorMsg and job.ErrorData
// in addition to returning any error.
// StartedAt, StoppedAt, and UpdatedAt are not modified.
func DoJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = errs.Errorf("job worker panic: %w", errs.AsErrorWithDebugStack(p))
		}
		errs.WrapWithFuncParams(&err, job)
	}()

	if job == nil {
		return errs.New("can't do nil job")
	}

	workersMtx.RLock()
	worker, hasWorker := workers[job.Type]
	workersMtx.RUnlock()

	if !hasWorker {
		return errs.Errorf("no worker for job of type '%s'", job.Type)
	}

	result, jobErr := worker.DoJob(ctx, job)
	if jobErr != nil {
		errorTitle := errs.Root(jobErr).Error()
		if nl := strings.IndexByte(errorTitle, '\n'); nl > 0 {
			// Only use first line of error message as errorTitle
			errorTitle = errorTitle[:nl]
		}
		errorTitle = strings.TrimSpace(errorTitle)

		OnError(jobErr)
		log.Errorf("Job error: %s", errorTitle).
			UUID("jobID", job.ID).
			Any("job", job).
			Err(jobErr).
			Log()

		job.ErrorMsg.Set(jobErr.Error())
		job.ErrorData, err = nullable.MarshalJSON(result)
		return errors.Join(jobErr, err)

	}

	job.Result, err = nullable.MarshalJSON(result)
	return err
}

func doJobAndSaveResultInDB(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, job)

	err = DoJob(ctx, job)
	if err != nil {
		e := db.SetJobError(ctx, job.ID, job.ErrorMsg.Get(), job.ErrorData)
		if e != nil {
			OnError(e)
			log.Error("Error while updating job error in the database").
				UUID("jobID", job.ID).
				Any("job", job).
				Err(e).
				Log()
			return e
		}
		// Return no error because it was saved in the database
		return nil
	}

	return db.SetJobResult(ctx, job.ID, job.Result)
}
