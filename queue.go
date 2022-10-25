package jobqueue

import (
	"context"

	"github.com/domonda/go-types/uu"
)

type Queue interface {
	SetListener(context.Context, QueueListener) error

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
