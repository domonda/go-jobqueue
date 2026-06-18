package jobworker

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
)

func noopWorker(context.Context, *jobqueue.Job) (any, error) { return nil, nil }

// resetWorkerRegistryState clears the package-global worker registry and the
// RegisteredJobTypes cache so each test starts from a known state, and restores
// it on cleanup. The jobworker package keeps registrations in process-global maps,
// so tests that register must isolate themselves explicitly. It mutates the maps
// directly (under workersMtx) rather than via Unregister so it works regardless of
// the running-threads guard.
func resetWorkerRegistryState(t *testing.T) {
	t.Helper()
	clear := func() {
		workersMtx.Lock()
		for k := range workers {
			delete(workers, k)
		}
		workerTypes = nil
		workerTypesGeneration = 0
		workersMtx.Unlock()
	}
	clear()
	t.Cleanup(clear)
}

// TestRegisteredJobTypesCacheAndGeneration covers the sorted output, the
// generation bump on every Register/Unregister, and that a repeated read is a
// cache hit returning the same backing array (no re-allocation).
func TestRegisteredJobTypesCacheAndGeneration(t *testing.T) {
	resetWorkerRegistryState(t)

	// Empty registry: a rebuilt cache is empty and the generation starts at zero.
	types, gen0 := RegisteredJobTypes()
	assert.Empty(t, types)
	assert.Zero(t, gen0)

	// Each Register bumps the generation once; the result is sorted regardless of
	// registration order.
	Register("email", noopWorker)
	Register("alpha", noopWorker)
	types, gen2 := RegisteredJobTypes()
	assert.Equal(t, []string{"alpha", "email"}, types, "types are sorted")
	assert.Equal(t, uint64(2), gen2, "generation bumped once per Register")

	// A repeated read hits the cache: same generation and the same backing array.
	types2, gen2b := RegisteredJobTypes()
	require.Equal(t, gen2, gen2b)
	require.NotEmpty(t, types2)
	assert.Same(t, &types[0], &types2[0], "repeated read returns the cached slice, no realloc")

	// Unregister also bumps the generation and removes the type.
	Unregister("alpha")
	types3, gen3 := RegisteredJobTypes()
	assert.Equal(t, []string{"email"}, types3)
	assert.Equal(t, uint64(3), gen3, "generation bumped on Unregister")
}

// TestRegisteredJobTypesEmptyRebuildIsCached pins the doc's "empty-but-non-nil
// means rebuilt, no job types" invariant: the first read of an empty registry
// rebuilds a non-nil empty slice, so the next read is a fast-path cache hit rather
// than another rebuild.
func TestRegisteredJobTypesEmptyRebuildIsCached(t *testing.T) {
	resetWorkerRegistryState(t)

	types1, gen1 := RegisteredJobTypes()
	assert.Empty(t, types1)

	workersMtx.RLock()
	cacheIsNonNil := workerTypes != nil
	workersMtx.RUnlock()
	assert.True(t, cacheIsNonNil, "empty registry still produces a non-nil cache")

	types2, gen2 := RegisteredJobTypes()
	assert.Equal(t, gen1, gen2, "no generation change for a cache hit")
	assert.Empty(t, types2)
}

// TestRegisteredJobTypesConcurrentAccess exercises the double-checked-locking read
// path against concurrent invalidation. Run with -race to catch a torn read or an
// unsynchronized cache rebuild; every returned slice must stay sorted.
func TestRegisteredJobTypesConcurrentAccess(t *testing.T) {
	resetWorkerRegistryState(t)

	const readers = 8
	stop := make(chan struct{})

	// Writer: churn one registration to keep invalidating the cache. Register and
	// Unregister alternate, so the type is never double-registered.
	var writerWG sync.WaitGroup
	writerWG.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				Register("churn", noopWorker)
				Unregister("churn")
			}
		}
	})

	var readersWG sync.WaitGroup
	for range readers {
		readersWG.Go(func() {
			for range 5000 {
				types, _ := RegisteredJobTypes()
				if !slices.IsSorted(types) {
					t.Errorf("RegisteredJobTypes returned unsorted slice: %v", types)
					return
				}
			}
		})
	}

	readersWG.Wait()
	close(stop)
	writerWG.Wait()
}

// TestRegisterUnregisterPanicWhileThreadsRunning verifies the startup-only guard:
// mutating the registry while worker threads run would race the jobworkerdb claim
// statement cache, so Register and Unregister must panic. The running threads are
// simulated by setting the counter directly (same package) to avoid needing a real
// database.
func TestRegisterUnregisterPanicWhileThreadsRunning(t *testing.T) {
	resetWorkerRegistryState(t)

	setupMtx.Lock()
	numRunningThreads = 1
	setupMtx.Unlock()
	t.Cleanup(func() {
		setupMtx.Lock()
		numRunningThreads = 0
		setupMtx.Unlock()
	})

	assert.Panics(t, func() { Register("late", noopWorker) },
		"Register must panic while worker threads are running")
	assert.Panics(t, func() { Unregister("late") },
		"Unregister must panic while worker threads are running")
}
