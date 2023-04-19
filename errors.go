package jobqueue

import (
	"context"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/uu"
)

const (
	ErrNotInitialized errs.Sentinel = "jobqueue service not initialized"
	ErrClosed         errs.Sentinel = "jobqueue is closed"
)

var _ Service = errService{}

type errService struct {
	err error
}

func ServiceWithError(err error) Service {
	return errService{err}
}

func (e errService) SetListener(context.Context, ServiceListener) error           { return e.err }
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
func (e errService) DeleteFinishedJobs(ctx context.Context) error                 { return e.err }
func (e errService) Close() error                                                 { return e.err }
