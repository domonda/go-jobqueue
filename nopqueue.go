package jobqueue

import (
	"context"

	"github.com/domonda/go-types/uu"
)

var _ Queue = NopQueue{}

// NopQueue is a Queue implementation
// that does nothing and returns nil for
// all its method result values.
type NopQueue struct{}

func (NopQueue) SetListener(context.Context, QueueListener) error             { return nil }
func (NopQueue) AddJob(ctx context.Context, job *Job) error                   { return nil }
func (NopQueue) GetJob(ctx context.Context, jobID uu.ID) (*Job, error)        { return nil, nil }
func (NopQueue) DeleteJob(ctx context.Context, jobID uu.ID) error             { return nil }
func (NopQueue) ResetJob(ctx context.Context, jobID uu.ID) error              { return nil }
func (NopQueue) ResetJobs(ctx context.Context, jobIDs uu.IDs) error           { return nil }
func (NopQueue) AddJobBundle(ctx context.Context, jobBundle *JobBundle) error { return nil }
func (NopQueue) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error) {
	return nil, nil
}
func (NopQueue) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error { return nil }
func (NopQueue) GetStatus(context.Context) (*Status, error)                   { return nil, nil }
func (NopQueue) GetAllJobsToDo(context.Context) ([]*Job, error)               { return nil, nil }
func (NopQueue) GetAllJobsWithErrors(context.Context) ([]*Job, error)         { return nil, nil }
func (NopQueue) DeleteFinishedJobs(ctx context.Context) error                 { return nil }
func (NopQueue) Close() error                                                 { return nil }
