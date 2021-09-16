package jobqueue

import (
	"context"
	"fmt"

	"github.com/domonda/go-types/uu"
)

var service Service

func SetService(ctx context.Context, s Service) error {
	err := s.SetListener(ctx, serviceListener{})
	if err != nil {
		return err
	}
	service = s
	return nil
}

func Close() error {
	if service == nil {
		return ErrClosed
	}
	return service.Close()
}

type Service interface {
	SetListener(context.Context, ServiceListener) error

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

type Status struct {
	NumJobs       int
	NumJobBundles int
	// NumWorkerThreads int
}

// IsZero returns true if the reciver is nil
// or dereferenced equal to its zero value.
// Valid to call on a nil receiver.
func (s *Status) IsZero() bool {
	return s == nil || *s == Status{}
}

// String implements the fmt.Stringer interface.
// Valid to call on a nil receiver.
func (s *Status) String() string {
	if s == nil {
		return "nil Status"
	}
	// return fmt.Sprintf("Status{NumJobs: %d, NumJobBundles: %d, NumWorkerThreads: %d}", s.NumJobs, s.NumJobBundles, s.NumWorkerThreads)
	return fmt.Sprintf("Status{NumJobs: %d, NumJobBundles: %d}", s.NumJobs, s.NumJobBundles)
}

func GetStatus(ctx context.Context) (status *Status, err error) {
	return service.GetStatus(ctx)
}

func GetAllJobsToDo(ctx context.Context) (jobs []*Job, err error) {
	return service.GetAllJobsToDo(ctx)
}

func GetAllJobsWithErrors(ctx context.Context) (jobs []*Job, err error) {
	return service.GetAllJobsWithErrors(ctx)
}
