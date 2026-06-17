package jobqueue

import (
	"context"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/uu"
)

// ServiceListener receives job and job bundle completion events from a Service.
// A Service forwards these events to the package-level job and job bundle
// stopped listeners; see NewDefaultServiceListener for the standard implementation.
type ServiceListener interface {
	// OnJobStopped is called when a job has stopped.
	// willRetry indicates that the job will be retried
	// and this is not the final stop.
	// Note: when willRetry is true, the job may already have been
	// reset for retry by the time the handler reads it from the database,
	// so its state may no longer reflect the error that triggered this notification.
	OnJobStopped(ctx context.Context, jobID uu.ID, jobType, jobOrigin string, willRetry bool)

	// OnJobBundleStopped is called when every job in a job bundle has stopped.
	OnJobBundleStopped(ctx context.Context, jobBundleID uu.ID, jobBundleType, jobBundleOrigin string)
}

// NewDefaultServiceListener returns the standard ServiceListener implementation.
// It loads the affected job or job bundle from service and dispatches to the
// listeners registered via AddJobStoppedListener, AddJobBundleStoppedListener,
// and SetJobBundleOfTypeStoppedListener. A bundle that completed without errors
// is deleted from the queue.
func NewDefaultServiceListener(service Service) ServiceListener {
	return defaultServiceListener{Service: service}
}

type defaultServiceListener struct {
	Service
}

func (l defaultServiceListener) OnJobStopped(ctx context.Context, jobID uu.ID, jobType, jobOrigin string, willRetry bool) {
	job, err := l.Service.GetJob(ctx, jobID)
	if err != nil {
		if errs.IsErrNotFound(err) {
			log.Warn("OnJobStopped called for an already deleted job").
				UUID("jobID", jobID).
				Str("jobType", jobType).
				Str("jobOrigin", jobOrigin).
				Log()
		} else {
			log.ErrorCtx(ctx, "OnJobStopped GetJob error").
				Err(err).
				UUID("jobID", jobID).
				Str("jobType", jobType).
				Str("jobOrigin", jobOrigin).
				Log()
		}
		return
	}

	jobStoppedListenersMtx.RLock()
	listeners := jobStoppedListeners
	jobStoppedListenersMtx.RUnlock()

	for _, listener := range listeners {
		listener.OnJobStopped(job, willRetry)
	}
}

func (l defaultServiceListener) OnJobBundleStopped(ctx context.Context, jobBundleID uu.ID, jobBundleType, jobBundleOrigin string) {
	jobBundle, err := l.Service.GetJobBundle(ctx, jobBundleID)
	if err != nil {
		log.ErrorCtx(ctx, "OnJobBundleStopped GetJobBundle error, ignoring and continuing...").
			Err(err).
			UUID("jobBundleID", jobBundleID).
			Str("jobBundleType", jobBundleType).
			Str("jobBundleOrigin", jobBundleOrigin).
			Log()
		return
	}

	// jobBundle.Jobs, err = service.GetJobBundleJobs(jobBundleID)
	// if err != nil {
	// 	log.ErrorCtx(ctx, ).Err(err).UUID("jobBundleID", jobBundleID).Msg("GetJobBundle")
	// 	return
	// }

	jobBundleStoppedListenersMtx.RLock()
	allTypesListeners := jobBundleStoppedListeners
	jobBundleStoppedListenersMtx.RUnlock()

	for _, listener := range allTypesListeners {
		listener.OnJobBundleStopped(jobBundle)
	}

	jobBundleOfTypeStoppedListenersMtx.RLock()
	typeListener := jobBundleOfTypeStoppedListeners[jobBundleType]
	jobBundleOfTypeStoppedListenersMtx.RUnlock()

	if typeListener != nil {
		typeListener.OnJobBundleStopped(jobBundle)
	}

	if !jobBundle.HasError() {
		err = l.Service.DeleteJobBundle(ctx, jobBundleID)
		if err != nil {
			log.ErrorCtx(ctx, "OnJobBundleStopped DeleteJobBundle error").
				Err(err).
				UUID("jobBundleID", jobBundleID).
				Str("jobBundleType", jobBundleType).
				Str("jobBundleOrigin", jobBundleOrigin).
				Log()
		}
	}
}
