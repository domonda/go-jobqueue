package jobqueue

import (
	"context"
	"errors"

	"github.com/domonda/go-types/uu"
)

var _ Service = DoNothingService{}

// DoNothingService is a Service implementation
// that does nothing and returns nil for
// all its method result values.
type DoNothingService struct{}

func (DoNothingService) AddListener(context.Context, ServiceListener) error {
	log.Info("DoNothingService.AddListener").Log()
	return nil
}

func (DoNothingService) AddJob(ctx context.Context, job *Job) error {
	log.Info("DoNothingService.AddJob").Log()
	return nil
}

func (DoNothingService) GetJob(ctx context.Context, jobID uu.ID) (*Job, error) {
	return nil, errors.New("DoNothingService.GetJob can't return jobs")
}

func (DoNothingService) DeleteJob(ctx context.Context, jobID uu.ID) error {
	log.Info("DoNothingService.DeleteJob").Log()
	return nil
}

func (DoNothingService) ResetJob(ctx context.Context, jobID uu.ID) error {
	log.Info("DoNothingService.ResetJob").Log()
	return nil
}

func (DoNothingService) ResetJobs(ctx context.Context, jobIDs uu.IDs) error {
	log.Info("DoNothingService.ResetJobs").Log()
	return nil
}

func (DoNothingService) AddJobBundle(ctx context.Context, jobBundle *JobBundle) error {
	log.Info("DoNothingService.AddJobBundle").Log()
	return nil
}

func (DoNothingService) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error) {
	return nil, errors.New("DoNothingService.GetJobBundle can't return jobs bundles")
}

func (DoNothingService) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error {
	log.Info("DoNothingService.DeleteJobBundle").Log()
	return nil
}

func (DoNothingService) GetStatus(context.Context) (*Status, error) {
	log.Info("DoNothingService.GetStatus").Log()
	return new(Status), nil
}

func (DoNothingService) GetAllJobsToDo(context.Context) ([]*Job, error) {
	log.Info("DoNothingService.GetAllJobsToDo").Log()
	return nil, nil
}

func (DoNothingService) GetAllJobsWithErrors(context.Context) ([]*Job, error) {
	log.Info("DoNothingService.GetAllJobsWithErrors").Log()
	return nil, nil
}

func (DoNothingService) DeleteFinishedJobs(ctx context.Context) error {
	log.Info("DoNothingService.DeleteFinishedJobs").Log()
	return nil
}

func (DoNothingService) Close() error {
	return nil
}
