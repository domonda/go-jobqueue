package jobqueue

import (
	"context"
	"errors"
	"time"

	"github.com/domonda/go-types/uu"
)

// DoNothingService returns a Service
// that does nothing except logging method calls
// and returning nil for all result values.
func DoNothingService() Service {
	return doNothingService{}
}

type doNothingService struct{}

func (doNothingService) AddListener(context.Context, ServiceListener) error {
	log.Info("DoNothingService.AddListener").Log()
	return nil
}

func (doNothingService) AddJob(ctx context.Context, job *Job) error {
	log.Info("DoNothingService.AddJob").Log()
	return nil
}

func (doNothingService) GetJob(ctx context.Context, jobID uu.ID) (*Job, error) {
	return nil, errors.New("DoNothingService.GetJob can't return jobs")
}

func (doNothingService) DeleteJob(ctx context.Context, jobID uu.ID) error {
	log.Info("DoNothingService.DeleteJob").Log()
	return nil
}

func (doNothingService) ResetJob(ctx context.Context, jobID uu.ID) error {
	log.Info("DoNothingService.ResetJob").Log()
	return nil
}

func (doNothingService) ResetJobs(ctx context.Context, jobIDs uu.IDs) error {
	log.Info("DoNothingService.ResetJobs").Log()
	return nil
}

func (doNothingService) AddJobBundle(ctx context.Context, jobBundle *JobBundle) error {
	log.Info("DoNothingService.AddJobBundle").Log()
	return nil
}

func (doNothingService) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (*JobBundle, error) {
	return nil, errors.New("DoNothingService.GetJobBundle can't return jobs bundles")
}

func (doNothingService) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) error {
	log.Info("DoNothingService.DeleteJobBundle").Log()
	return nil
}

func (doNothingService) GetStatus(context.Context) (*Status, error) {
	log.Info("DoNothingService.GetStatus").Log()
	return new(Status), nil
}

func (doNothingService) GetAllJobsToDo(context.Context) ([]*Job, error) {
	log.Info("DoNothingService.GetAllJobsToDo").Log()
	return nil, nil
}

func (doNothingService) GetAllJobsStartedBefore(ctx context.Context, since time.Time) ([]*Job, error) {
	log.Info("DoNothingService.GetAllJobsStartedBefore").Log()
	return nil, nil
}

func (doNothingService) GetAllJobsWithErrors(context.Context) ([]*Job, error) {
	log.Info("DoNothingService.GetAllJobsWithErrors").Log()
	return nil, nil
}

func (doNothingService) DeleteFinishedJobs(ctx context.Context) error {
	log.Info("DoNothingService.DeleteFinishedJobs").Log()
	return nil
}

func (doNothingService) Close() error {
	log.Info("DoNothingService.Close").Log()
	return nil
}
