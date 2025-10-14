package jobqueue

import (
	"context"
	"time"

	"github.com/domonda/go-types/uu"
)

var (
	defaultService Service = ServiceWithError(ErrNotInitialized)
	serviceCtxKey  int
)

// SetDefaultService sets the default job queue service used when no service
// is found in the context. This should be called during application initialization.
// Panics if service is nil.
func SetDefaultService(service Service) {
	if service == nil {
		panic("jobqueue.SetDefaultService(nil)")
	}
	defaultService = service
}

// ContextWithService returns a new context with the given service attached.
// The service can be retrieved using GetService or ServiceFromContextOrNil.
func ContextWithService(ctx context.Context, service Service) context.Context {
	return context.WithValue(ctx, &serviceCtxKey, service)
}

// ServiceFromContextOrNil returns the Service from the context or nil if not found.
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

// Close closes the default service and releases any resources.
func Close() error {
	if defaultService == nil {
		return nil
	}
	return defaultService.Close()
}

// Service is the main interface for interacting with the job queue.
// Implementations handle job persistence, retrieval, and lifecycle management.
type Service interface {
	// AddListener registers a listener for job and job bundle completion events.
	AddListener(context.Context, ServiceListener) error

	// AddJob adds a new job to the queue.
	AddJob(ctx context.Context, job *Job) error

	// GetJob retrieves a job by its ID.
	GetJob(ctx context.Context, jobID uu.ID) (*Job, error)

	// DeleteJob deletes a job from the queue.
	DeleteJob(ctx context.Context, jobID uu.ID) error

	// ResetJob resets the processing state of a job in the queue
	// so that the job is ready to be re-processed.
	ResetJob(ctx context.Context, jobID uu.ID) error

	// ResetJobs resets the processing state of multiple jobs in the queue
	// so that they are ready to be re-processed.
	ResetJobs(ctx context.Context, jobIDs uu.IDs) error

	// AddJobBundle adds a new job bundle with all its jobs to the queue.
	AddJobBundle(ctx context.Context, jobBundle *JobBundle) error

	// GetJobBundle retrieves a job bundle by its ID, including all its jobs.
	GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error)

	// DeleteJobBundle deletes a job bundle and all its jobs from the queue.
	DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error

	// GetStatus returns the current queue status with job and bundle counts.
	GetStatus(context.Context) (*Status, error)

	// GetAllJobsToDo returns all jobs that are ready to be processed.
	GetAllJobsToDo(context.Context) ([]*Job, error)

	// GetAllJobsStartedBefore returns all jobs that started before the given time and haven't stopped.
	GetAllJobsStartedBefore(ctx context.Context, since time.Time) ([]*Job, error)

	// GetAllJobsWithErrors returns all jobs that have errors.
	GetAllJobsWithErrors(context.Context) ([]*Job, error)

	// DeleteFinishedJobs deletes all successfully completed jobs without errors.
	DeleteFinishedJobs(ctx context.Context) error

	// Close closes the service and releases any resources.
	Close() error
}

// Add adds a job to the queue using the service from the context or the default service.
func Add(ctx context.Context, job *Job) error {
	return GetService(ctx).AddJob(ctx, job)
}

// GetJob retrieves a job by its ID using the service from the context or the default service.
func GetJob(ctx context.Context, jobID uu.ID) (*Job, error) {
	return GetService(ctx).GetJob(ctx, jobID)
}

// DeleteFinishedJobs deletes all successfully completed jobs using the service from the context or the default service.
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

// GetAllJobsToDo returns all jobs that are ready to be processed using the service from the context or the default service.
func GetAllJobsToDo(ctx context.Context) (jobs []*Job, err error) {
	return GetService(ctx).GetAllJobsToDo(ctx)
}

// GetAllJobsStartedBefore returns all jobs that started before the given time using the service from the context or the default service.
func GetAllJobsStartedBefore(ctx context.Context, since time.Time) (jobs []*Job, err error) {
	return GetService(ctx).GetAllJobsStartedBefore(ctx, since)
}

// GetAllJobsWithErrors returns all jobs that have errors using the service from the context or the default service.
func GetAllJobsWithErrors(ctx context.Context) (jobs []*Job, err error) {
	return GetService(ctx).GetAllJobsWithErrors(ctx)
}

// GetStatus returns the current queue status using the service from the context or the default service.
func GetStatus(ctx context.Context) (status *Status, err error) {
	return GetService(ctx).GetStatus(ctx)
}

// AddBundle adds a job bundle to the queue using the service from the context or the default service.
func AddBundle(ctx context.Context, jobBundle *JobBundle) (err error) {
	return GetService(ctx).AddJobBundle(ctx, jobBundle)
}

// GetJobBundle retrieves a job bundle by its ID using the service from the context or the default service.
func GetJobBundle(ctx context.Context, jobBundleID uu.ID) (jobBundle *JobBundle, err error) {
	return GetService(ctx).GetJobBundle(ctx, jobBundleID)
}
