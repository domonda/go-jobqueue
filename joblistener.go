package jobqueue

import (
	"sync"
)

// JobStoppedListener is notified when a job has stopped.
type JobStoppedListener interface {
	// OnJobStopped is called when a job has stopped.
	// willRetry indicates that the job will be retried
	// and this is not the final stop.
	// Note: when willRetry is true, the job may already have been
	// reset for retry by the time this handler is called,
	// so its state may no longer reflect the error that triggered this notification.
	OnJobStopped(job *Job, willRetry bool)
}

// JobStoppedListenerFunc adapts a plain function to the JobStoppedListener interface.
type JobStoppedListenerFunc func(job *Job, willRetry bool)

// OnJobStopped calls f, implementing JobStoppedListener.
func (f JobStoppedListenerFunc) OnJobStopped(job *Job, willRetry bool) {
	f(job, willRetry)
}

var (
	jobStoppedListeners    []JobStoppedListener
	jobStoppedListenersMtx sync.RWMutex
)

// AddJobStoppedListener registers a listener that is called whenever a job stops.
func AddJobStoppedListener(listener JobStoppedListener) {
	jobStoppedListenersMtx.Lock()
	defer jobStoppedListenersMtx.Unlock()

	jobStoppedListeners = append(jobStoppedListeners, listener)
}

// RemoveJobStoppedListener removes a listener previously registered with
// AddJobStoppedListener. It does nothing if the listener is not registered.
func RemoveJobStoppedListener(listener JobStoppedListener) {
	jobStoppedListenersMtx.Lock()
	defer jobStoppedListenersMtx.Unlock()

	for i := range jobStoppedListeners {
		if jobStoppedListeners[i] == listener {
			jobStoppedListeners = append(jobStoppedListeners[:i], jobStoppedListeners[i+1:]...)
			return
		}
	}
}
