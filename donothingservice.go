package jobqueue

import (
	"context"

	"github.com/domonda/go-types/uu"
)

var _ Service = DoNothingService{}

// DoNothingService is a Service implementation
// that does nothing and returns nil for
// all its method result values.
type DoNothingService struct{}

func (DoNothingService) SetListener(context.Context, ServiceListener) error           { return nil }
func (DoNothingService) AddJob(ctx context.Context, job *Job) error                   { return nil }
func (DoNothingService) GetJob(ctx context.Context, jobID uu.ID) (*Job, error)        { return nil, nil }
func (DoNothingService) DeleteJob(ctx context.Context, jobID uu.ID) error             { return nil }
func (DoNothingService) ResetJob(ctx context.Context, jobID uu.ID) error              { return nil }
func (DoNothingService) ResetJobs(ctx context.Context, jobIDs uu.IDs) error           { return nil }
func (DoNothingService) AddJobBundle(ctx context.Context, jobBundle *JobBundle) error { return nil }
func (DoNothingService) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error) {
	return nil, nil
}
func (DoNothingService) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error { return nil }
func (DoNothingService) GetStatus(context.Context) (*Status, error)                   { return nil, nil }
func (DoNothingService) GetAllJobsToDo(context.Context) ([]*Job, error)               { return nil, nil }
func (DoNothingService) GetAllJobsWithErrors(context.Context) ([]*Job, error)         { return nil, nil }
func (DoNothingService) DeleteFinishedJobs(ctx context.Context) error                 { return nil }
func (DoNothingService) Close() error
