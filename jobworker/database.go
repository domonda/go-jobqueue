package jobworker

import (
	"context"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
)

var db DataBase

func SetDataBase(newDB DataBase) {
	db = newDB
}

type DataBase interface {
	jobqueue.Service

	SetJobAvailableListener(context.Context, func()) error
	StartNextJobOrNil(ctx context.Context) (*jobqueue.Job, error)

	SetJobError(ctx context.Context, jobID uu.ID, errorMsg string, errorData nullable.JSON) error
	SetJobResult(ctx context.Context, jobID uu.ID, result nullable.JSON) error
	SetJobStart(ctx context.Context, jobID uu.ID, startAt time.Time) error

	DeleteJobsFromOrigin(ctx context.Context, origin string) error
	DeleteJobsOfType(ctx context.Context, jobType string) error
	DeleteJobBundlesFromOrigin(ctx context.Context, origin string) error
	DeleteJobBundlesOfType(ctx context.Context, bundleType string) error
	DeleteAllJobsAndBundles(ctx context.Context) error

	// CompletedJobBundleOrNil(jobBundleID uu.ID) (*jobqueue.JobBundle, error)
	// GetJobResult(jobID uu.ID) (nullable.JSON, error)
	// SetJobIssue(jobID uu.ID, issueType string, issueData nullable.JSON) error
}

func DeleteAllJobsAndBundles(ctx context.Context) error {
	if db == nil {
		return errs.New("no DataBase defined")
	}
	return db.DeleteAllJobsAndBundles(ctx)
}
