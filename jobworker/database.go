package jobworker

import (
	"context"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
)

// DataBase is the persistence backend the jobworker package needs in addition
// to the jobqueue.Service interface. The jobworkerdb package provides the
// PostgreSQL implementation and registers it with SetDataBase.
type DataBase interface {
	jobqueue.Service

	// SetJobAvailableListener registers a callback that is invoked whenever a new
	// job becomes available, or clears the callback when passed nil.
	SetJobAvailableListener(context.Context, func()) error

	// StartNextJobOrNil claims and starts the next available job, returning nil
	// if no job is currently available.
	StartNextJobOrNil(ctx context.Context) (*jobqueue.Job, error)

	// SetJobError stops the job with a terminal error described by errorMsg and
	// optional errorData, marking it as not to be retried.
	SetJobError(ctx context.Context, jobID uu.ID, errorMsg string, errorData nullable.JSON) error

	// SetJobResult stops the job successfully and stores its result.
	SetJobResult(ctx context.Context, jobID uu.ID, result nullable.JSON) error

	// SetJobStart reschedules the job to start no earlier than startAt, clearing
	// any previous start, stop, and error state.
	SetJobStart(ctx context.Context, jobID uu.ID, startAt time.Time) error

	// SetJobWorkerAlive updates the worker_alive_at heartbeat timestamp of a job
	// that is currently being processed.
	SetJobWorkerAlive(ctx context.Context, jobID uu.ID) error

	// ScheduleRetry reschedules the job to run again at startAt with the given
	// retryCount, clearing any previous start, stop, and error state.
	ScheduleRetry(ctx context.Context, jobID uu.ID, startAt time.Time, retryCount int) error

	// DeleteJobsFromOrigin deletes all jobs created from the given origin.
	DeleteJobsFromOrigin(ctx context.Context, origin string) error

	// DeleteJobsOfType deletes all jobs of the given type.
	DeleteJobsOfType(ctx context.Context, jobType string) error

	// DeleteJobBundlesFromOrigin deletes all job bundles created from the given origin.
	DeleteJobBundlesFromOrigin(ctx context.Context, origin string) error

	// DeleteJobBundlesOfType deletes all job bundles of the given type.
	DeleteJobBundlesOfType(ctx context.Context, bundleType string) error

	// DeleteAllJobsAndBundles deletes all jobs and job bundles from the queue.
	DeleteAllJobsAndBundles(ctx context.Context) error
}

// DeleteAllJobsAndBundles deletes all jobs and job bundles from the queue using
// the DataBase set with SetDataBase. It returns an error if no DataBase has been set.
func DeleteAllJobsAndBundles(ctx context.Context) error {
	if db == nil {
		return errs.New("no DataBase defined")
	}
	return db.DeleteAllJobsAndBundles(ctx)
}
