package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
)

// JobBundle represents a group of related jobs that are tracked together.
// The bundle provides a way to wait for completion of all jobs and receive
// a single notification when all jobs are finished.
type JobBundle struct {
	// ID is the unique identifier for the job bundle.
	ID uu.ID `db:"id,pk" json:"id"`

	// Type categorizes the job bundle for filtering and identification.
	Type string `db:"type" json:"type"` // CHECK(length("type") > 0)

	// Origin identifies the source or context that created the bundle.
	Origin string `db:"origin" json:"origin"` // CHECK(length(origin) > 0)

	// NumJobs is the total number of jobs in the bundle.
	NumJobs int `db:"num_jobs" json:"num_jobs"`

	// NumJobsStopped tracks how many jobs have completed (successfully or with errors).
	NumJobsStopped int `db:"num_jobs_stopped" json:"num_jobs_stopped"`

	// UpdatedAt is the last time the bundle was modified.
	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`

	// CreatedAt is when the bundle was created.
	CreatedAt time.Time `db:"created_at" json:"createdAt"`

	// Jobs contains the individual jobs in the bundle. This field is not stored in the database.
	Jobs []*Job `db:"-" json:"jobs,omitempty"`
}

// HasError returns true if the receiver is not nil
// and any of the bundle's Jobs HasError method returns true.
// Valid to call on a nil receiver.
func (b *JobBundle) HasError() bool {
	if b == nil {
		return false
	}
	for _, job := range b.Jobs {
		if job.HasError() {
			return true
		}
	}
	return false
}

// String implements the fmt.Stringer interface.
// Valid to call on a nil receiver.
func (b *JobBundle) String() string {
	if b == nil {
		return "nil JobBundle"
	}
	return fmt.Sprintf("JobBundle %s, type %s, created at %s from origin '%s'", b.ID, b.Type, b.CreatedAt, b.Origin)
}

// NewJobBundle creates a new job bundle with the specified type and origin.
// A job will be created for every JobDesc in the jobDescriptions slice.
// If a JobDesc.Type is an empty string, ReflectJobTypeOfPayload will be used to determine the type.
// If startAt is not null, none of the jobs in the bundle will start before that time.
// Returns an error if jobDescriptions is empty or if any job creation fails.
func NewJobBundle(ctx context.Context, jobBundleType, jobBundleOrigin string, jobDescriptions []JobDesc, startAt nullable.Time) (*JobBundle, error) {
	if len(jobDescriptions) == 0 {
		return nil, errors.New("no jobDescriptions")
	}

	numJobs := len(jobDescriptions)
	jobs := make([]*Job, numJobs)
	for i, desc := range jobDescriptions {
		jobType := desc.Type
		if jobType == "" {
			jobType = ReflectJobTypeOfPayload(desc.Payload)
		}
		job, err := NewJobWithPriority(uu.NewID(ctx), jobType, desc.Origin, desc.Payload, desc.Priority, startAt)
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}

	jobBundle := &JobBundle{
		ID:      uu.NewID(ctx),
		Type:    jobBundleType,
		Origin:  jobBundleOrigin,
		Jobs:    jobs,
		NumJobs: numJobs,
	}

	return jobBundle, nil
}
