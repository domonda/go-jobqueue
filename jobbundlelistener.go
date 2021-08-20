package jobqueue

import (
	"sync"
)

type JobBundleStoppedListener interface {
	OnJobBundleStopped(jobBundle *JobBundle)
}

type JobBundleStoppedListenerFunc func(jobBundle *JobBundle)

func (f JobBundleStoppedListenerFunc) OnJobBundleStopped(jobBundle *JobBundle) {
	f(jobBundle)
}

var (
	jobBundleStoppedListeners    []JobBundleStoppedListener
	jobBundleStoppedListenersMtx sync.RWMutex

	jobBundleOfTypeStoppedListeners    = make(map[string]JobBundleStoppedListener)
	jobBundleOfTypeStoppedListenersMtx sync.RWMutex
)

func AddJobBundleStoppedListener(listener JobBundleStoppedListener) {
	jobBundleStoppedListenersMtx.Lock()
	defer jobBundleStoppedListenersMtx.Unlock()

	jobBundleStoppedListeners = append(jobBundleStoppedListeners, listener)
}

func RemoveJobBundleStoppedListener(listener JobBundleStoppedListener) {
	jobBundleStoppedListenersMtx.Lock()
	defer jobBundleStoppedListenersMtx.Unlock()

	for i := range jobBundleStoppedListeners {
		if jobBundleStoppedListeners[i] == listener {
			jobBundleStoppedListeners = append(jobBundleStoppedListeners[:i], jobBundleStoppedListeners[i+1:]...)
			return
		}
	}
}

func SetJobBundleOfTypeStoppedListener(jobBundleType string, listener JobBundleStoppedListener) {
	jobBundleOfTypeStoppedListenersMtx.Lock()
	defer jobBundleOfTypeStoppedListenersMtx.Unlock()

	jobBundleOfTypeStoppedListeners[jobBundleType] = listener
}

func RemoveJobBundleOfTypeStoppedListener(jobBundleType string, listener JobBundleStoppedListener) {
	jobBundleOfTypeStoppedListenersMtx.Lock()
	defer jobBundleOfTypeStoppedListenersMtx.Unlock()

	delete(jobBundleOfTypeStoppedListeners, jobBundleType)
}

func RemoveAllJobBundleStoppedListeners() {
	jobBundleStoppedListenersMtx.Lock()
	jobBundleStoppedListeners = nil
	jobBundleStoppedListenersMtx.Unlock()

	jobBundleOfTypeStoppedListenersMtx.Lock()
	for jobBundleType := range jobBundleOfTypeStoppedListeners {
		delete(jobBundleOfTypeStoppedListeners, jobBundleType)
	}
	jobBundleOfTypeStoppedListenersMtx.Unlock()
}
