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
	HeartbeatInterval = 10 * time.Second

	typeOfError   = reflect.TypeFor[error]()
	typeOfContext = reflect.TypeFor[context.Context]()
)

func OverrideLogger(logger *golog.Logger) {
	log = logger
}
