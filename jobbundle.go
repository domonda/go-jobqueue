package jobqueue

import (
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
func NewJobBundle(jobBundleType, jobBundleOrigin string, jobDescriptions []JobDesc, startAt nullable.Time) (*JobBundle, error) {
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
		job, err := NewJobWithPriority(jobType, desc.Origin, desc.Payload, desc.Priority, startAt)
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}

	jobBundle := &JobBundle{
		ID:      uu.IDv4(),
		Type:    jobBundleType,
		Origin:  jobBundleOrigin,
		Jobs:    jobs,
		NumJobs: numJobs,
	}

	return jobBundle, nil
}

// func GetJobBundleJobs(jobBundleID uu.ID) (jobs []*Job, err error) {
// 	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID)

// 	return service.GetJobBundleJobs(jobBundleID)
// }

// func DeleteJobBundlesOfType(bundleType string) error {
// 	return db.DeleteJobBundlesOfType(bundleType)
// }

// func DeleteJobBundlesFromOrigin(origin string) error {
// 	return db.DeleteJobBundlesFromOrigin(origin)
// }

// func AddJobBundleReportResult(jobBundleType, jobBundleOrigin string, jobDescriptions []JobDesc) (jobBundleID uu.ID, resultChan <-chan *JobBundle, err error) {
// 	defer errs.WrapWithFuncParams(&err, ctx, jobBundleType, jobBundleOrigin, jobDescriptions)

// 	if len(jobDescriptions) == 0 {
// 		return uu.IDNil, nil, errs.New("no jobDescriptions")
// 	}

// 	jobs := make([]*Job, len(jobDescriptions))
// 	for i, desc := range jobDescriptions {
// 		jobType := desc.Type
// 		if jobType == "" {
// 			jobType = ReflectJobType(desc.Payload)
// 		}
// 		jobs[i], err = newJob(jobType, desc.Payload, desc.Priority, desc.Origin)
// 		if err != nil {
// 			return uu.IDNil, nil, err
// 		}
// 	}

// 	jobBundle := &JobBundle{
// 		ID:     uu.IDv4(),
// 		Type:   jobBundleType,
// 		Origin: jobBundleOrigin,
// 		Jobs:   jobs,
// NumJobs: len(jobs),
// 	}

// 	return service.AddJobBundleReportResult(jobBundle)
// }

// AddJobBundleReflectJobs adds a job bundle of jobBundleType from jobsAndBundleSource.
// A job will be added for every payload with the passed priority, of ReflectJobType(payload) and the origin jobsAndBundleSource.
// func AddJobBundleReflectJobs(jobBundleID uu.ID, jobBundleType, jobsAndBundleSource string, priority int64, jobPayloads ...any) (jobBundleID uu.ID, err error) {
// 	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID, jobBundleType, jobsAndBundleSource, priority, jobPayloads)

// 	if len(jobPayloads) == 0 {
// 		return uu.IDNil, errs.New("no jobPayloads")
// 	}

// 	jobs := make([]*Job, len(jobPayloads))
// 	for i, payload := range jobPayloads {
// 		jobType := ReflectJobType(payload)
// 		jobs[i], err = newJob(jobType, payload, priority, jobsAndBundleSource)
// 		if err != nil {
// 			return uu.IDNil, err
// 		}
// 	}

// 	return service.AddJobBundle(jobBundleID, jobBundleType, jobsAndBundleSource, jobs)
// }

// func AddJobBundleReflectJobsReportResult(jobBundleID uu.ID, jobBundleType, jobsAndBundleSource string, priority int64, jobPayloads ...any) (jobBundleID uu.ID, resultChan <-chan *JobBundle, err error) {
// 	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID, jobBundleType, jobsAndBundleSource, priority, jobPayloads)

// 	if len(jobPayloads) == 0 {
// 		return uu.IDNil, nil, errs.New("no jobPayloads")
// 	}

// 	jobs := make([]*Job, len(jobPayloads))
// 	for i, payload := range jobPayloads {
// 		jobType := ReflectJobType(payload)
// 		jobs[i], err = newJob(jobType, payload, priority, jobsAndBundleSource)
// 		if err != nil {
// 			return uu.IDNil, nil, err
// 		}
// 	}

// 	return service.AddJobBundleReportResult(jobBundleID, jobBundleType, jobsAndBundleSource, jobs)
// }
