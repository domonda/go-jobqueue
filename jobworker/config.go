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

	typeOfError   = reflect.TypeOf((*error)(nil)).Elem()
	typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()
)

func OverrideLogger(logger *golog.Logger) {
	log = logger
}
