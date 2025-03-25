package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
)

type JobBundle struct {
	ID uu.ID `db:"id,pk" json:"id"`

	Type   string `db:"type"   json:"type"`   // CHECK(length("type") > 0)
	Origin string `db:"origin" json:"origin"` // CHECK(length(origin) > 0)

	NumJobs        int `db:"num_jobs"         json:"num_jobs"`
	NumJobsStopped int `db:"num_jobs_stopped" json:"num_jobs_stopped"`

	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`

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

// NewJobBundle xxxx TODO adds a job bundle of jobBundleType from jobBundleOrigin.
// A job will be added for every JobDesc.
// If a JobDesc.Type is an empty string, then ReflectJobType(JobDesc.Payload) will be used instead.
// If startAt is not null then the job bundle will not start before that time.
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
