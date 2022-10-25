package jobqueue

import (
	"context"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/uu"
)

const (
	ErrClosed errs.Sentinel = "job queue closed"
)

type errQueue struct {
	err error
}

func QueueWithError(err error) Queue {
	return errQueue{err}
}

func (e errQueue) SetListener(context.Context, QueueListener) error             { return e.err }
func (e errQueue) AddJob(ctx context.Context, job *Job) error                   { return e.err }
func (e errQueue) GetJob(ctx context.Context, jobID uu.ID) (*Job, error)        { return nil, e.err }
func (e errQueue) DeleteJob(ctx context.Context, jobID uu.ID) error             { return e.err }
func (e errQueue) ResetJob(ctx context.Context, jobID uu.ID) error              { return e.err }
func (e errQueue) ResetJobs(ctx context.Context, jobIDs uu.IDs) error           { return e.err }
func (e errQueue) AddJobBundle(ctx context.Context, jobBundle *JobBundle) error { return e.err }
func (e errQueue) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error) {
	return nil, e.err
}
func (e errQueue) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error { return e.err }
func (e errQueue) GetStatus(context.Context) (*Status, error)                   { return nil, e.err }
func (e errQueue) GetAllJobsToDo(context.Context) ([]*Job, error)               { return nil, e.err }
func (e errQueue) GetAllJobsWithErrors(context.Context) ([]*Job, error)         { return nil, e.err }
func (e errQueue) DeleteFinishedJobs(ctx context.Context) error                 { return e.err }
func (e errQueue) Close() error                                                 { return e.err }
