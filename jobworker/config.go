package jobworker

import (
	"context"
	"reflect"

	rootlog "github.com/domonda/golog/log"
)

var (
	log = rootlog.NewPackageLogger("jobworker")

	// OnError will be called for every error that
	// would also be logged.
	OnError = func(error) {}

	typeOfError   = reflect.TypeOf((*error)(nil)).Elem()
	typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()
)
