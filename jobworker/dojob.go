package jobworker

import (
	"context"
	"errors"
	"strings"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/golog"
)

// DoJob does a job synchronously and sets the job.Result
// if there was no error or sets job.ErrorMsg and job.ErrorData
// in addition to returning any error.
//
// The job.ID is added to the context that's passed to the
// job worker function as golog attribute with the key "jobID".
//
// If the passed context already has a deadline, it will be respected.
// Otherwise, JobTimeout is applied if configured.
//
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

	jobCtx := golog.ContextWithAttribs(ctx, golog.NewUUID("jobID", job.ID))

	// Apply timeout if configured and context doesn't already have a deadline
	if JobTimeout > 0 {
		if _, hasDeadline := jobCtx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			jobCtx, cancel = context.WithTimeout(jobCtx, JobTimeout)
			defer cancel()
		}
	}

	result, jobErr := worker(jobCtx, job)
	if jobErr != nil {
		errorTitle := errs.Root(jobErr).Error()
		if nl := strings.IndexByte(errorTitle, '\n'); nl > 0 {
			// Only use first line of error message as errorTitle
			errorTitle = errorTitle[:nl]
		}
		errorTitle = strings.TrimSpace(errorTitle)

		OnError(jobErr)

		if job.CurrentRetryCount >= job.MaxRetryCount {
			log.ErrorfCtx(jobCtx, "Job error: %s", errorTitle).
				Any("job", job).
				Err(jobErr).
				Log()
		} else {
			log.WarnfCtx(jobCtx, "Job error: %s", errorTitle).
				Any("job", job).
				Err(jobErr).
				Log()
		}

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

	if err == nil {
		return db.SetJobResult(ctx, job.ID, job.Result)
	}

	// job.ErrorMsg might be null if DoJob returns an error
	// that was not returned from the jobworker but from
	// some other job-queue logic error,
	errorMsg := job.ErrorMsg.StringOr(err.Error())
	err = db.SetJobError(ctx, job.ID, errorMsg, job.ErrorData)

	if err != nil {
		OnError(err)
		log.ErrorCtx(ctx, "Error while updating job error in the database").
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()
		return err
	}

	if job.CurrentRetryCount >= job.MaxRetryCount {
		return err
	}

	scheduleRetry, ok := retrySchedulers[job.Type]

	if !ok {
		err = errs.New("Retry scheduler doesn't exist for job")
		OnError(err)
		log.ErrorCtx(ctx, "Retry scheduler doesn't exist for job").
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()
		return err
	}

	nextStart, err := scheduleRetry(ctx, job)

	if err != nil {
		OnError(err)
		log.ErrorCtx(ctx, "Retry scheduler returned an error").
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()
		return err
	}

	if err := db.ScheduleRetry(ctx, job.ID, nextStart, job.CurrentRetryCount+1); err != nil {
		OnError(err)
		log.ErrorCtx(ctx, "Could not schedule retry for job").
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()

		return err
	}

	return err
}
