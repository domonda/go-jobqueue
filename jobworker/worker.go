package jobworker

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ungerik/go-command"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-types"
	"github.com/domonda/go-types/notnull"
)

type Worker interface {
	DoJob(ctx context.Context, job *jobqueue.Job) (result interface{}, err error)
}

type WorkerFunc func(ctx context.Context, job *jobqueue.Job) (result interface{}, err error)

func (f WorkerFunc) DoJob(ctx context.Context, job *jobqueue.Job) (result interface{}, err error) {
	return f(ctx, job)
}

// Register a Worker implementation for a jobType.
// See also RegisterFunc
func Register(jobType string, worker Worker) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType)

	workersMtx.Lock()
	defer workersMtx.Unlock()

	if _, exists := workers[jobType]; exists {
		panic(fmt.Errorf("a worker for jobType %#v has already been registered", jobType))
	}

	workers[jobType] = worker
}

// IsRegistered checks if a worker is registered for the given job type.
func IsRegistered(jobType string) bool {
	workersMtx.RLock()
	defer workersMtx.RUnlock()

	return workers[jobType] != nil
}

func RegisteredJobTypes() notnull.StringArray {
	workersMtx.RLock()
	defer workersMtx.RUnlock()

	jobTypes := make(notnull.StringArray, 0, len(workers))
	for jobType := range workers {
		jobTypes = append(jobTypes, jobType)
	}
	return jobTypes
}

// RegisterFunc uses reflection to register a function with a custom
// payload argument type as Worker for jobs of type ReflectJobType(arg).
// The playload JSON of the job will be unmarshalled to the type of the argument.
func RegisterFunc(workerFunc interface{}) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), workerFunc)

	registerFunc("REFLECT_PAYLOAD_TYPE", workerFunc)
}

// RegisterFuncForJobType uses reflection to register a function with a custom
// payload argument type as Worker for jobs of jobType.
// The playload JSON of the job will be unmarshalled to the type of the argument.
func RegisterFuncForJobType(jobType string, workerFunc interface{}) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType, workerFunc)

	if jobType == "" {
		panic(fmt.Errorf("jobType must not be empty"))
	}

	registerFunc(jobType, workerFunc)
}

// registerFunc uses jobType = reflectJobType(payloadType) if jobType is "REFLECT_PAYLOAD_TYPE"
func registerFunc(jobType string, workerFunc interface{}) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType, workerFunc)

	workerFuncVal := reflect.ValueOf(workerFunc)
	workerFuncType := workerFuncVal.Type()
	if workerFuncType.Kind() != reflect.Func {
		panic(fmt.Errorf("workerFunc is not a function but %T", workerFunc))
	}

	// Check argument
	if workerFuncType.NumIn() != 1 {
		panic(fmt.Errorf("workerFunc must have 1 argument, but has %d", workerFuncType.NumIn()))
	}
	argType := workerFuncType.In(0)
	if !types.CanMarshalJSON(argType) {
		panic(fmt.Errorf("workerFunc must have an argument type that can be marshalled to JSON, but has %s", argType))
	}
	payloadType := argType
	for payloadType.Kind() == reflect.Ptr {
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

	Register(jobType, WorkerFunc(func(ctx context.Context, job *jobqueue.Job) (result interface{}, err error) {
		var payloadVal reflect.Value
		switch payloadType.Kind() {
		case reflect.Struct, reflect.Slice:
			payloadVal = reflect.New(payloadType)
		case reflect.Map:
			payloadVal = reflect.MakeMap(payloadType)
		default:
			panic("unsupported payload type")
		}
		err = job.Payload.UnmarshalTo(payloadVal.Interface())
		if err != nil {
			return nil, fmt.Errorf("Error while unmarshalling job payload '%s': %w", job.Payload, err)
		}
		results := workerFuncVal.Call([]reflect.Value{payloadVal})
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
		}
		panic("unsupported number of results")
	}))
}

// RegisterCommand registers a command as worker for a jobType.
// The value of every top level key in the JSON job payload will be
// assigned to the argument with the key name.
// The job queue only handles errors returned from worker functions.
// If the command returns something else in addition to an error,
// then resultsHandlers will be called as opportunity to handle
// those results and not loose them.
func RegisterCommand(jobType string, commandFunc interface{}, args command.Args, resultsHandlers ...command.ResultsHandler) {
	defer errs.LogPanicWithFuncParams(log.ErrorWriter(), jobType)

	f, err := command.GetJSONArgsFunc(commandFunc, args, resultsHandlers...)
	if err != nil {
		panic(err)
	}

	Register(jobType, WorkerFunc(func(ctx context.Context, job *jobqueue.Job) (result interface{}, err error) {
		return nil, f(ctx, job.Payload)
	}))
}

func Unregister(jobTypes ...string) {
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
}
