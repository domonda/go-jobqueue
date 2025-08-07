package jobworker

import (
	"context"
	"reflect"

	"github.com/domonda/golog"
	rootlog "github.com/domonda/golog/log"
)

var (
	log = rootlog.NewPackageLogger()

	// OnError will be called for every error that
	// would also be logged.
	OnError = func(error) {}

	typeOfError   = reflect.TypeOf((*error)(nil)).Elem()
	typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()
)

func OverrideLogger(logger *golog.Logger) {
	log = logger
}
