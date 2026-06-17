package jobqueue

import (
	"context"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/uu"
)

const (
	// ErrNotInitialized is returned by Service operations when no job queue
	// service has been initialized. It is the error wrapped by the default
	// service until SetDefaultService installs a real one.
	ErrNotInitialized errs.Sentinel = "jobqueue service not initialized"

	// ErrClosed is returned by Service operations after the service has been closed.
	ErrClosed errs.Sentinel = "jobqueue is closed"
)

var _ Service = errService{}

type errService struct {
	err error
}

// ServiceWithError returns a Service whose every method returns the given err.
// It is used as a placeholder default service before a real one is installed
// with SetDefaultService, so that calls fail with a clear error such as
// ErrNotInitialized instead of a nil pointer panic.
func ServiceWithError(err error) Service {
	return errService{err}
}

func (e errService) AddListener(context.Context, ServiceListener) error           { return e.err }
func (e errService) AddJob(ctx context.Context, job *Job) error                   { return e.err }
func (e errService) GetJob(ctx context.Context, jobID uu.ID) (*Job, error)        { return nil, e.err }
func (e errService) DeleteJob(ctx context.Context, jobID uu.ID) error             { return e.err }
func (e errService) ResetJob(ctx context.Context, jobID uu.ID) error              { return e.err }
func (e errService) ResetJobs(ctx context.Context, jobIDs uu.IDs) error           { return e.err }
func (e errService) AddJobBundle(ctx context.Context, jobBundle *JobBundle) error { return e.err }
func (e errService) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error) {
	return nil, e.err
}
func (e errService) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error { return e.err }
func (e errService) GetStatus(context.Context) (*Status, error)                   { return nil, e.err }
func (e errService) GetAllJobsToDo(context.Context) ([]*Job, error)               { return nil, e.err }
func (e errService) GetAllJobsWithErrors(context.Context) ([]*Job, error)         { return nil, e.err }
func (e errService) GetAllJobsStartedBefore(ctx context.Context, since time.Time) ([]*Job, error) {
	return nil, e.err
}
func (e errService) DeleteFinishedJobs(ctx context.Context) error { return e.err }
func (e errService) Close() error                                 { return e.err }
