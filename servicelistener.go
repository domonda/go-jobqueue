package jobqueue

import (
	"context"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/uu"
)

type ServiceListener interface {
	OnJobStopped(ctx context.Context, jobID uu.ID, jobType, jobOrigin string)
	OnJobBundleStopped(ctx context.Context, jobBundleID uu.ID, jobBundleType, jobBundleOrigin string)
}

type serviceListener struct{}

func (serviceListener) OnJobStopped(ctx context.Context, jobID uu.ID, jobType, jobOrigin string) {
	job, err := service.GetJob(ctx, jobID)
	if err != nil {
		if errs.IsErrNotFound(err) {
			log.Warn("OnJobStopped called for an already deleted job").
				UUID("jobID", jobID).
				Str("jobType", jobType).
				Str("jobOrigin", jobOrigin).
				Log()
		} else {
			log.Error("OnJobStopped GetJob error").
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
		listener.OnJobStopped(job)
	}
}

func (serviceListener) OnJobBundleStopped(ctx context.Context, jobBundleID uu.ID, jobBundleType, jobBundleOrigin string) {
	jobBundle, err := service.GetJobBundle(ctx, jobBundleID)
	if err != nil {
		log.Error("OnJobBundleStopped GetJobBundle error, ignoring and continuing...").
			Err(err).
			UUID("jobBundleID", jobBundleID).
			Str("jobBundleType", jobBundleType).
			Str("jobBundleOrigin", jobBundleOrigin).
			Log()
		return
	}

	// jobBundle.Jobs, err = service.GetJobBundleJobs(jobBundleID)
	// if err != nil {
	// 	log.Error().Err(err).UUID("jobBundleID", jobBundleID).Msg("GetJobBundle")
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
		err = service.DeleteJobBundle(ctx, jobBundleID)
		if err != nil {
			log.Error("OnJobBundleStopped DeleteJobBundle error").
				Err(err).
				UUID("jobBundleID", jobBundleID).
				Str("jobBundleType", jobBundleType).
				Str("jobBundleOrigin", jobBundleOrigin).
				Log()
		}
	}
}
