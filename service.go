package jobqueue

import (
	"context"

	"github.com/domonda/go-types/uu"
)

var (
	defaultService Service = ServiceWithError(ErrNotInitialized)
	serviceCtxKey  int
)

func SetDefaultService(service Service) {
	if service == nil {
		panic("jobqueue.SetDefaultService(nil)")
	}
	defaultService = service
}

func ContextWithService(ctx context.Context, service Service) context.Context {
	return context.WithValue(ctx, &serviceCtxKey, service)
}

func ServiceFromContextOrNil(ctx context.Context) Service {
	s, _ := ctx.Value(&serviceCtxKey).(Service)
	return s
}

// GetService returns a Service from the context that was
// added using ContextWithService or the default service
// that is configurable with SetDefaultService.
func GetService(ctx context.Context) Service {
	if s := ServiceFromContextOrNil(ctx); s != nil {
		return s
	}
	return defaultService
}

type Service interface {
	AddListener(context.Context, ServiceListener) error

	AddJob(ctx context.Context, job *Job) error
	GetJob(ctx context.Context, jobID uu.ID) (*Job, error)

	// DeleteJob deletes a job from the queue.
	DeleteJob(ctx context.Context, jobID uu.ID) error

	// ResetJob resets the processing state of a job in the queue
	// so that the job is ready to be re-processed.
	ResetJob(ctx context.Context, jobID uu.ID) error

	// ResetJobs resets the processing state of multiple jobs in the queue
	// so that they are ready to be re-processed.
	ResetJobs(ctx context.Context, jobIDs uu.IDs) error

	AddJobBundle(ctx context.Context, jobBundle *JobBundle) error
	GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error)
	DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error

	GetStatus(context.Context) (*Status, error)
	GetAllJobsToDo(context.Context) ([]*Job, error)
	GetAllJobsWithErrors(context.Context) ([]*Job, error)
	DeleteFinishedJobs(ctx context.Context) error
	Close() error
}

func Add(ctx context.Context, job *Job) error {
	return GetService(ctx).AddJob(ctx, job)
}

func GetJob(ctx context.Context, jobID uu.ID) (*Job, error) {
	return GetService(ctx).GetJob(ctx, jobID)
}

func DeleteFinishedJobs(ctx context.Context) error {
	return GetService(ctx).DeleteFinishedJobs(ctx)
}

// ResetJob resets the processing state of a job in the queue
// so that the job is ready to be re-processed.
func ResetJob(ctx context.Context, jobID uu.ID) error {
	return GetService(ctx).ResetJob(ctx, jobID)
}

// ResetJobs resets the processing state of multiple jobs in the queue
// so that they are ready to be re-processed.
func ResetJobs(ctx context.Context, jobIDs uu.IDSlice) error {
	return GetService(ctx).ResetJobs(ctx, jobIDs)
}

// DeleteJob deletes a job from the queue.
func DeleteJob(ctx context.Context, jobID uu.ID) error {
	return GetService(ctx).DeleteJob(ctx, jobID)
}

func GetAllJobsToDo(ctx context.Context) (jobs []*Job, err error) {
	return GetService(ctx).GetAllJobsToDo(ctx)
}

func GetAllJobsWithErrors(ctx context.Context) (jobs []*Job, err error) {
	return GetService(ctx).GetAllJobsWithErrors(ctx)
}

func GetStatus(ctx context.Context) (status *Status, err error) {
	return GetService(ctx).GetStatus(ctx)
}

func AddBundle(ctx context.Context, jobBundle *JobBundle) (err error) {
	return GetService(ctx).AddJobBundle(ctx, jobBundle)
}

func GetJobBundle(ctx context.Context, jobBundleID uu.ID) (jobBundle *JobBundle, err error) {
	return GetService(ctx).GetJobBundle(ctx, jobBundleID)
}
