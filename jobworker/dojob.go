package jobworker

import (
	"context"
	"errors"
	"strings"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/golog"

	"github.com/domonda/go-jobqueue"
)

// DoJob runs the worker registered for job.Type synchronously in the calling
// goroutine and sets job.Result on success, or job.ErrorMsg and job.ErrorData
// on failure, in addition to returning any error.
//
// DoJob does not access the database: it neither claims the job nor persists
// the outcome. It can therefore be used to run a job synchronously and inline,
// for example for synchronous jobs (see jobworkerdb.ContextWithSynchronousJobs)
// or in tests. Worker threads use doJobAndSaveResultInDB instead, which calls
// DoJob and then persists the outcome and handles retries.
//
// A panic in the job worker function is recovered and returned as an error,
// so DoJob always returns normally and never panics out to its caller.
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
			err = errors.Join(err, errs.Errorf("job worker panic: %w", errs.AsErrorWithDebugStack(p)))
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

// doJobAndSaveResultInDB runs a previously claimed job with DoJob and persists
// the outcome in the database. It is the entry point used by the worker threads
// (see worker in workerthreads.go), as opposed to the database-free DoJob.
//
// A heartbeat keeps the job's worker_alive_at timestamp fresh for as long as
// this worker owns the job — both while DoJob runs and while the outcome is
// finalized — so a crashed worker can be detected.
//
// Depending on the outcome it:
//   - stores the result via SetJobResult on success;
//   - resets the job via ResetJob if the context was cancelled (e.g. shutdown),
//     so it is retried without consuming a retry attempt;
//   - schedules a retry via ScheduleRetry if retries remain; or
//   - stores the error via SetJobError once all retries are exhausted.
//
// The retry scheduler runs while the heartbeat is still alive, and the job is
// marked stopped only by a single terminal write, so a slow scheduler cannot
// leave the job in a stopped+errored state that another process's reaper would
// reclaim out from under this worker.
//
// The database writes use context.WithoutCancel so that a cancelled context
// (e.g. during shutdown) does not prevent the job's final state from being
// persisted.
func doJobAndSaveResultInDB(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, job)
	defer errs.RecoverPanicAsError(&err)

	// Start a heartbeat that periodically updates the job's worker_alive_at
	// timestamp so observers can detect whether this worker still owns the job —
	// while DoJob runs and while the outcome is finalized below — or has crashed.
	//
	// The heartbeat is kept alive across the ENTIRE finalization, including the
	// user-registered retry scheduler, so the job stays claimed-and-live
	// (started_at set, stopped_at NULL, fresh worker_alive_at) until a single
	// terminal write transitions it. A slow retry scheduler therefore cannot
	// expose the job to another process's reaper: the reaper's in-progress branch
	// only reclaims a STALE heartbeat, and the job is never put into a separate
	// stopped+errored state for its error→retry branch to match.
	//
	// Each terminal branch calls stopHeartbeat() right before its database write.
	// stopHeartbeat blocks until the heartbeat goroutine has exited, so no
	// worker_alive_at update races the transition. It is idempotent and also
	// deferred as a leak-safety net in case a future change adds a path that
	// returns without the explicit call.
	stopHeartbeat := startJobHeartbeat(ctx, job.ID)
	defer stopHeartbeat()

	jobErr := DoJob(ctx, job)

	if jobErr == nil {
		stopHeartbeat()
		return db.SetJobResult(context.WithoutCancel(ctx), job.ID, job.Result)
	}

	// Reset the job without consuming a retry attempt when it was interrupted
	// rather than having genuinely failed, so it can be picked up again later.
	//
	// ctx is the worker thread's context, so ctx.Err() covers both cancellation
	// and deadline-exceeded of that context (e.g. a shutdown that cancels the
	// context passed to StartThreads, or a deadline on it). The second clause
	// additionally catches a worker that returned a cancellation error while
	// ctx itself is still alive.
	//
	// A context.DeadlineExceeded carried by jobErr while ctx is still alive is
	// deliberately NOT treated as an interruption: it originates from the
	// per-job JobTimeout applied inside DoJob, which means the job genuinely ran
	// too long. Such a job falls through to the normal error/retry handling
	// below so it consumes a retry instead of being reset (and retried) forever.
	// Adding `|| errors.Is(jobErr, context.DeadlineExceeded)` here would also
	// reset per-job timeouts, but is intentionally avoided for that reason: a
	// job that always times out would otherwise be reset and retried endlessly
	// without ever consuming a retry attempt or failing permanently.
	if ctx.Err() != nil || errors.Is(jobErr, context.Canceled) {
		stopHeartbeat()
		resetErr := db.ResetJob(context.WithoutCancel(ctx), job.ID)
		if resetErr != nil {
			OnError(resetErr)
			log.ErrorCtx(ctx, "Error resetting job after context cancellation").
				UUID("jobID", job.ID).
				Err(resetErr).
				Log()
		}
		return ctx.Err()
	}

	// job.ErrorMsg might be null if DoJob returns an error
	// that was not returned from the jobworker but from
	// some other job-queue logic error.
	errorMsg := job.ErrorMsg.StringOr(jobErr.Error())

	// Genuine final failure: no retries remain. Mark the job errored as the
	// single terminal write (SetJobError also counts the job in its bundle).
	if job.CurrentRetryCount >= job.MaxRetryCount {
		stopHeartbeat()
		err = db.SetJobError(context.WithoutCancel(ctx), job.ID, errorMsg, job.ErrorData)
		if err != nil {
			OnError(err)
			log.ErrorCtx(ctx, "Error while updating job error in the database").
				UUID("jobID", job.ID).
				Any("job", job).
				Err(err).
				Log()
			return err
		}
		return nil
	}

	// Retryable: run the (possibly slow) retry scheduler while the heartbeat is
	// still alive, so the job stays claimed-and-live and no other process can
	// reclaim it. Only once the next start time is known is the heartbeat stopped
	// and the job moved to the scheduled-retry state by a single ScheduleRetry
	// write — the job is never put into a separate stopped+errored state, so there
	// is no SetJobError→ScheduleRetry window for another process's reaper to
	// exploit.
	retrySchedulersMtx.RLock()
	scheduleRetry, ok := retrySchedulers[job.Type]
	retrySchedulersMtx.RUnlock()
	if !ok {
		stopHeartbeat()
		err = errs.New("Retry scheduler doesn't exist for job")
		OnError(err)
		log.ErrorCtx(ctx, "Retry scheduler doesn't exist for job").
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()
		// Can't reschedule: mark the job errored so the misconfiguration is
		// visible and the job is reclaimable once a scheduler is registered.
		if setErr := db.SetJobError(context.WithoutCancel(ctx), job.ID, errorMsg, job.ErrorData); setErr != nil {
			OnError(setErr)
		}
		return err
	}

	nextStart, err := scheduleRetry(ctx, job)
	if err != nil {
		stopHeartbeat()
		OnError(err)
		log.ErrorCtx(ctx, "Retry scheduler returned an error").
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()
		// Couldn't compute the next start: mark the job errored so the failure is
		// visible and reclaimable rather than leaving it silently in-progress.
		if setErr := db.SetJobError(context.WithoutCancel(ctx), job.ID, errorMsg, job.ErrorData); setErr != nil {
			OnError(setErr)
		}
		return err
	}

	// Stop the heartbeat before the terminal write so no worker_alive_at update
	// races the transition. Use WithoutCancel so context cancellation between the
	// checks above and this call cannot leave the job stuck without a retry.
	stopHeartbeat()
	err = db.ScheduleRetry(context.WithoutCancel(ctx), job.ID, nextStart, job.CurrentRetryCount+1)
	if err != nil {
		OnError(err)
		log.ErrorCtx(ctx, "Could not schedule retry for job").
			UUID("jobID", job.ID).
			Any("job", job).
			Err(err).
			Log()
		return err
	}

	return nil
}
