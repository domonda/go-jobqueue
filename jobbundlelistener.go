package jobqueue

import (
	"sync"
)

// JobBundleStoppedListener is notified when all jobs in a job bundle have stopped.
type JobBundleStoppedListener interface {
	// OnJobBundleStopped is called once every job in the bundle has stopped.
	OnJobBundleStopped(jobBundle *JobBundle)
}

// JobBundleStoppedListenerFunc adapts a plain function to the
// JobBundleStoppedListener interface.
type JobBundleStoppedListenerFunc func(jobBundle *JobBundle)

// OnJobBundleStopped calls f, implementing JobBundleStoppedListener.
func (f JobBundleStoppedListenerFunc) OnJobBundleStopped(jobBundle *JobBundle) {
	f(jobBundle)
}

var (
	jobBundleStoppedListeners    []JobBundleStoppedListener
	jobBundleStoppedListenersMtx sync.RWMutex

	jobBundleOfTypeStoppedListeners    = make(map[string]JobBundleStoppedListener)
	jobBundleOfTypeStoppedListenersMtx sync.RWMutex
)

// AddJobBundleStoppedListener registers a listener that is called whenever any
// job bundle stops, regardless of its type.
func AddJobBundleStoppedListener(listener JobBundleStoppedListener) {
	jobBundleStoppedListenersMtx.Lock()
	defer jobBundleStoppedListenersMtx.Unlock()

	jobBundleStoppedListeners = append(jobBundleStoppedListeners, listener)
}

// RemoveJobBundleStoppedListener removes a listener previously registered with
// AddJobBundleStoppedListener. It does nothing if the listener is not registered.
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

// SetJobBundleOfTypeStoppedListener registers a listener that is called when a
// job bundle of the given type stops. Only one listener can be set per type;
// setting a new one replaces any previous listener for that type.
func SetJobBundleOfTypeStoppedListener(jobBundleType string, listener JobBundleStoppedListener) {
	jobBundleOfTypeStoppedListenersMtx.Lock()
	defer jobBundleOfTypeStoppedListenersMtx.Unlock()

	jobBundleOfTypeStoppedListeners[jobBundleType] = listener
}

// RemoveJobBundleOfTypeStoppedListener removes the listener registered for the
// given job bundle type with SetJobBundleOfTypeStoppedListener. The listener
// argument is ignored, since only one listener exists per type.
func RemoveJobBundleOfTypeStoppedListener(jobBundleType string, listener JobBundleStoppedListener) {
	jobBundleOfTypeStoppedListenersMtx.Lock()
	defer jobBundleOfTypeStoppedListenersMtx.Unlock()

	delete(jobBundleOfTypeStoppedListeners, jobBundleType)
}

// RemoveAllJobBundleStoppedListeners removes all registered job bundle stopped
// listeners, both the global listeners and the per-type listeners.
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
