package jobworker

import (
	"context"
	"reflect"
	"time"

	"github.com/domonda/golog"
	rootlog "github.com/domonda/golog/log"
)

var (
	log = rootlog.NewPackageLogger()

	// OnError will be called for every error that
	// would also be logged.
	OnError = func(error) {}

	// JobTimeout is the timeout applied to the context passed to job worker functions.
	// Default is 15 minutes. Set to 0 to disable the timeout.
	JobTimeout = 15 * time.Minute

	// HeartbeatInterval is how often a worker updates the worker_alive_at
	// timestamp of the job it is processing, so that observers can tell whether
	// the worker is still alive or has crashed. Default is 10 seconds.
	// Set to 0 to disable heartbeats.
	//
	// HeartbeatInterval must be configured during startup, before StartThreads, and
	// treated as immutable afterwards. Whether a claim stamps worker_alive_at is
	// baked into the cached jobworkerdb claim statement the first time a job is
	// claimed (it reads HeartbeatInterval > 0 at that point), and the cache is not
	// re-prepared when this value changes. Toggling it across the 0 boundary at
	// runtime would make the claim path and the heartbeat/reaper logic disagree.
	HeartbeatInterval = 10 * time.Second

	// Set by SetDataBase
	db DataBase

	typeOfError   = reflect.TypeFor[error]()
	typeOfContext = reflect.TypeFor[context.Context]()

	// workerTypes caches the sorted set of registered job types derived from the
	// workers map and is guarded by workersMtx (declared in workerthreads.go). It is
	// invalidated by setting it back to nil whenever workers changes (Register/
	// Unregister, normally only at startup) and lazily rebuilt on the next
	// RegisteredJobTypes read. A nil workerTypes means the cache is invalid and must
	// be rebuilt; an empty-but-non-nil workerTypes means "rebuilt, no job types
	// registered".
	workerTypes []string

	// workerTypesGeneration is incremented (under workersMtx, in Register/Unregister)
	// every time the set of registered job types changes. RegisteredJobTypes returns
	// it alongside the types so a consumer can cache values derived from the types
	// and rebuild only when the generation changes — a single locked read yields a
	// consistent (types, generation) pair, so there is no torn read and no need to
	// compare the type sets element by element.
	workerTypesGeneration uint64
)

// OverrideLogger replaces the logger used by the jobworker package.
func OverrideLogger(logger *golog.Logger) {
	log = logger
}

// SetDataBase sets the DataBase implementation used by the jobworker package.
// It must be called during initialization before starting worker threads;
// jobworkerdb.InitJobQueue does this automatically.
func SetDataBase(newDB DataBase) {
	db = newDB
}
