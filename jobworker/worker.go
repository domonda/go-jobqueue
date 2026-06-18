package jobworker

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-sqldb"
	"github.com/domonda/go-types"

	"github.com/domonda/go-jobqueue"
)

// WorkerFunc processes a job and returns an optional result that is stored as
// the job's result, or an error if processing failed. Register a WorkerFunc for
// a job type with Register.
type WorkerFunc func(ctx context.Context, job *jobqueue.Job) (result any, err error)

// Register a Worker implementation for a jobType.
//
// Register must be called during startup, before StartThreads. It mutates the set
// of job types that jobworkerdb caches a prepared claim statement against (keyed
// on a generation bumped here), and changing that set while worker threads claim
// jobs would race that cache. Register panics if called while worker threads are
// running.
//
// See also RegisterFunc
func Register(jobType string, worker WorkerFunc) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType)

	if sqlInject, info := sqldb.IsSQLInjection(jobType); sqlInject {
		panic(fmt.Errorf("jobType %#v contains probably SQL injection: %s", jobType, info))
	}

	// Hold setupMtx for the whole call so StartThreads (which takes setupMtx for
	// writing) cannot begin between this guard and the workers mutation below.
	// Registration is a startup-only step; see the doc comment above.
	setupMtx.RLock()
	defer setupMtx.RUnlock()
	if numRunningThreads > 0 {
		panic(fmt.Errorf("jobworker.Register(%#v) must be called before StartThreads, but %d worker thread(s) are running", jobType, numRunningThreads))
	}

	workersMtx.Lock()
	defer workersMtx.Unlock()

	if _, exists := workers[jobType]; exists {
		panic(fmt.Errorf("a worker for jobType %#v has already been registered", jobType))
	}

	workers[jobType] = worker
	invalidateWorkerTypesCacheLocked()
}

// IsRegistered checks if a worker is registered for the given job type.
func IsRegistered(jobType string) bool {
	workersMtx.RLock()
	defer workersMtx.RUnlock()

	return workers[jobType] != nil
}

// RegisteredJobTypes returns the sorted job types that currently have a worker
// registered, together with a generation that changes whenever that set changes
// (via Register/Unregister). The pair comes from a single locked read, so it is
// always consistent: a consumer can cache a value derived from the types and
// rebuild only when the generation differs from the one it last built against,
// without comparing the type sets themselves.
//
// The result is read from a cache rebuilt only when the set of registered workers
// changes, so the common case is a single read lock with no allocation. The
// returned slice is shared and MUST NOT be mutated by the caller.
func RegisteredJobTypes() (jobTypes []string, generation uint64) {
	workersMtx.RLock()
	cached, generation := workerTypes, workerTypesGeneration
	workersMtx.RUnlock()
	if cached != nil {
		return cached, generation
	}

	workersMtx.Lock()
	defer workersMtx.Unlock()

	// Re-check under the write lock: another goroutine may have rebuilt the cache
	// between the RUnlock above and acquiring the write lock. The rebuilt slice is
	// sorted for a canonical order downstream consumers can cache against;
	// workerTypesGeneration is left untouched (it is bumped on invalidation, so it
	// already reflects the current set).
	if workerTypes == nil {
		types := make([]string, 0, len(workers))
		for jobType := range workers {
			types = append(types, jobType)
		}
		slices.Sort(types)
		workerTypes = types
	}
	return workerTypes, workerTypesGeneration
}

// invalidateWorkerTypesCacheLocked marks the registered-job-types cache stale and
// bumps the generation so downstream consumers (e.g. the jobworkerdb claim query)
// know to rebuild. The caller must hold workersMtx for writing.
func invalidateWorkerTypesCacheLocked() {
	workerTypes = nil
	workerTypesGeneration++
}

// RegisterFunc uses reflection to register a function with a custom
// payload argument type as Worker for jobs of type ReflectJobType(arg).
// The playload JSON of the job will be unmarshalled to the type of the argument.
//
// Like Register (which it calls), RegisterFunc must be called before StartThreads
// and panics if worker threads are running.
func RegisterFunc(workerFunc any) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), workerFunc)

	registerFunc("REFLECT_PAYLOAD_TYPE", workerFunc)
}

// RegisterFuncForJobType uses reflection to register a function with a custom
// payload argument type as Worker for jobs of jobType.
// The playload JSON of the job will be unmarshalled to the type of the argument.
//
// Like Register (which it calls), RegisterFuncForJobType must be called before
// StartThreads and panics if worker threads are running.
func RegisterFuncForJobType(jobType string, workerFunc any) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType, workerFunc)

	if jobType == "" {
		panic(fmt.Errorf("jobType must not be empty"))
	}

	registerFunc(jobType, workerFunc)
}

// registerFunc uses jobType = reflectJobType(payloadType) if jobType is "REFLECT_PAYLOAD_TYPE"
func registerFunc(jobType string, workerFunc any) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType, workerFunc)

	workerFuncVal := reflect.ValueOf(workerFunc)
	workerFuncType := workerFuncVal.Type()
	if workerFuncType.Kind() != reflect.Func {
		panic(fmt.Errorf("workerFunc is not a function but %T", workerFunc))
	}

	hasCtx := workerFuncType.NumIn() >= 1 && workerFuncType.In(0) == typeOfContext

	// Check argument
	if hasCtx {
		if workerFuncType.NumIn() != 2 {
			panic(fmt.Errorf("workerFunc must have 1 argument after context, but has %d", workerFuncType.NumIn()-1))
		}
	} else {
		if workerFuncType.NumIn() != 1 {
			panic(fmt.Errorf("workerFunc must have 1 argument, but has %d", workerFuncType.NumIn()))
		}
	}

	argType := workerFuncType.In(0)
	if hasCtx {
		argType = workerFuncType.In(1)
	}
	if !types.CanMarshalJSON(argType) {
		panic(fmt.Errorf("workerFunc must have an argument type that can be marshalled to JSON, but has %s", argType))
	}
	payloadType := argType
	for payloadType.Kind() == reflect.Pointer {
		payloadType = payloadType.Elem()
	}

	if jobType == "REFLECT_PAYLOAD_TYPE" {
		jobType = jobqueue.JobTypeOfPayloadType(payloadType)
	}

	resultIsError := false

	// Check result
	switch workerFuncType.NumOut() {
	case 0:
		// OK

	case 1:
		resultIsError = workerFuncType.Out(0) == typeOfError

	case 2:
		resultType := workerFuncType.Out(0)
		if !types.CanMarshalJSON(resultType) {
			panic(fmt.Errorf("workerFunc must have a first result type that can be marshalled to JSON, but has %s", resultType))
		}
		if workerFuncType.Out(1) != typeOfError {
			panic(fmt.Errorf("second workerFunc result must be of type error, but is %s", workerFuncType.Out(1)))
		}

	default:
		panic(fmt.Errorf("workerFunc must have 1 or 2 results, but has %d", workerFuncType.NumOut()))
	}

	Register(jobType, WorkerFunc(func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
		payloadVal := reflect.New(payloadType) // JSON unmarshalling always needs a pointer
		err = job.Payload.UnmarshalTo(payloadVal.Interface())
		if err != nil {
			return nil, fmt.Errorf("error while unmarshalling job payload '%s': %w", job.Payload, err)
		}
		if argType.Kind() != reflect.Pointer {
			payloadVal = payloadVal.Elem()
		}
		var params []reflect.Value
		if hasCtx {
			params = []reflect.Value{reflect.ValueOf(ctx), payloadVal}
		} else {
			params = []reflect.Value{payloadVal}
		}
		results := workerFuncVal.Call(params)
		switch len(results) {
		case 0:
			return nil, nil
		case 1:
			if resultIsError {
				return nil, errs.AsError(results[0].Interface())
			}
			return results[0].Interface(), nil
		case 2:
			return results[0].Interface(), errs.AsError(results[1].Interface())
		default:
			panic("unsupported number of results")
		}
	}))
}

// // RegisterCommand registers a command.Function as worker for a jobType.
// // The value of every top level key in the JSON job payload will be
// // assigned to the argument with the key name.
// // The job queue only handles errors returned from worker functions.
// // If the command.Function returns something else in addition to an error,
// // then resultsHandlers will be called as opportunity to handle
// // those results and not loose them.
// func RegisterCommand(jobType string, commandFunc command.Function, resultsHandlers ...command.ResultsHandler) {
// 	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType)

// 	f, err := command.NewJSONArgsFunc(commandFunc, resultsHandlers...)
// 	if err != nil {
// 		panic(err)
// 	}

// 	Register(jobType, WorkerFunc(func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
// 		return nil, f(ctx, job.Payload)
// 	}))
// }

// Unregister removes the workers for the given job types,
// or all registered workers if no job type is passed.
//
// Like Register, Unregister mutates the cached claim statement's job-type set, so
// it must be called while no worker threads are running: stop them with
// FinishThreads (or StopThreads) first. Unregister panics if worker threads are
// running.
func Unregister(jobTypes ...string) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobTypes)

	// Hold setupMtx for the whole call so StartThreads cannot begin between this
	// guard and the workers mutation below (mirrors Register).
	setupMtx.RLock()
	defer setupMtx.RUnlock()
	if numRunningThreads > 0 {
		panic(fmt.Errorf("jobworker.Unregister must be called after FinishThreads, but %d worker thread(s) are running", numRunningThreads))
	}

	workersMtx.Lock()
	defer workersMtx.Unlock()

	if len(jobTypes) > 0 {
		log.Debug("Unregister workers for job types").Strs("jobTypes", jobTypes).Log()
		for _, jobType := range jobTypes {
			delete(workers, jobType)
		}
	} else {
		log.Debug("Unregister all workers").Log()
		for jobType := range workers {
			delete(workers, jobType)
		}
	}
	invalidateWorkerTypesCacheLocked()
}
