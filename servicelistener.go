package jobqueue

import (
	"context"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/uu"
)

type ServiceListener interface {
	// OnJobStopped is called when a job has stopped.
	// willRetry indicates that the job will be retried
	// and this is not the final stop.
	// Note: when willRetry is true, the job may already have been
	// reset for retry by the time the handler reads it from the database,
	// so its state may no longer reflect the error that triggered this notification.
	OnJobStopped(ctx context.Context, jobID uu.ID, jobType, jobOrigin string, willRetry bool)
	OnJobBundleStopped(ctx context.Context, jobBundleID uu.ID, jobBundleType, jobBundleOrigin string)
}

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
