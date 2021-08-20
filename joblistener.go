package jobqueue

import (
	"sync"
)

type JobStoppedListener interface {
	OnJobStopped(job *Job)
}

type JobStoppedListenerFunc func(job *Job)

func (f JobStoppedListenerFunc) OnJobStopped(job *Job) {
	f(job)
}

var (
	jobStoppedListeners    []JobStoppedListener
	jobStoppedListenersMtx sync.RWMutex
)

func AddJobStoppedListener(listener JobStoppedListener) {
	jobStoppedListenersMtx.Lock()
	defer jobStoppedListenersMtx.Unlock()

	jobStoppedListeners = append(jobStoppedListeners, listener)
}

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
