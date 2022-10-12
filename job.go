package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/domonda/go-types/notnull"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
)

type Job struct {
	ID       uu.ID         `db:"id"        json:"id"`
	BundleID uu.NullableID `db:"bundle_id" json:"bundleId"`

	Type     string        `db:"type"     json:"type"` // CHECK(length("type") > 0 AND length("type") <= 100)
	Payload  notnull.JSON  `db:"payload"  json:"payload"`
	Priority int64         `db:"priority" json:"priority"`
	Origin   string        `db:"origin"   json:"origin"`  // CHECK(length(origin) > 0 AND length(origin) <= 100)
	StartAt  nullable.Time `db:"start_at" json:"startAt"` // If not NULL, earliest time to start the job

	StartedAt nullable.Time `db:"started_at" json:"startedAt"` // Time when started working on the job, or NULL when not started
	StoppedAt nullable.Time `db:"stopped_at" json:"stoppedAt"` // Time when working on job was stoped because of a decision question or an error, or NULL

	ErrorMsg  nullable.NonEmptyString `db:"error_msg"  json:"errorMsg"`  // If there was an error working off the job
	ErrorData nullable.JSON           `db:"error_data" json:"errorData"` // Optional error metadata
	Result    nullable.JSON           `db:"result"     json:"result"`    // Result if the job returned one

	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
}

// IsStarted returns if StartedAt is not null.
// Valid to call on a nil receiver.
func (j *Job) IsStarted() bool {
	if j == nil {
		return false
	}
	return j.StartedAt.IsNotNull()
}

// IsStopped returns if StoppedAt is not null.
// Valid to call on a nil receiver.
func (j *Job) IsStopped() bool {
	if j == nil {
		return false
	}
	return j.StoppedAt.IsNotNull()
}

// IsFinished returns if a job has been finished without an error.
// Valid to call on a nil receiver.
func (j *Job) IsFinished() bool {
	return j.IsStopped() && !j.HasError()
}

// HasError returns true if the receiver is not nil
// and has an ErrorMsg.
// Valid to call on a nil receiver.
func (j *Job) HasError() bool {
	return j != nil && j.ErrorMsg.IsNotNull()
}

// String implements the fmt.Stringer interface.
// Valid to call on a nil receiver.
func (j *Job) String() string {
	if j == nil {
		return "nil Job"
	}
	return fmt.Sprintf("Job %s, type %s, priority %d, created at %s from origin '%s'", j.ID, j.Type, j.Priority, j.CreatedAt, j.Origin)
}

// NewJobWithPriority creates a Job but does not add it to the queue.
// The passed payload will be marshalled to JSON or directly interpreted as JSON if possible.
// If startAt is not null then the job will not start before that time.
func NewJobWithPriority(jobType, origin string, payload any, priority int64, startAt nullable.Time) (*Job, error) {
	if jobType == "" {
		return nil, errors.New("empty jobType")
	}
	if payload == nil {
		return nil, errors.New("nil job payload")
	}

	var (
		payloadJSON notnull.JSON
		err         error
	)
	switch x := payload.(type) {
	case notnull.JSON:
		payloadJSON = x
		if !payloadJSON.Valid() {
			return nil, fmt.Errorf("job payload is not valid JSON: %#v", string(x))
		}

	case nullable.JSON:
		payloadJSON = notnull.JSON(x)
		if !payloadJSON.Valid() {
			return nil, fmt.Errorf("job payload is not valid JSON: %#v", string(x))
		}

	case json.RawMessage:
		payloadJSON = notnull.JSON(x)
		if !payloadJSON.Valid() {
			return nil, fmt.Errorf("job payload is not valid JSON: %#v", string(x))
		}

	case []byte:
		payloadJSON = notnull.JSON(x)
		if !payloadJSON.Valid() {
			return nil, fmt.Errorf("job payload is not valid JSON: %#v", string(x))
		}

	case string:
		payloadJSON = notnull.JSON(x)
		if !payloadJSON.Valid() {
			return nil, fmt.Errorf("job payload is not valid JSON: %#v", x)
		}

	case json.Marshaler:
		payloadJSON, err = x.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("job payload is not valid JSON: %#v, error: %w", x, err)
		}

	default:
		payloadJSON, err = notnull.MarshalJSON(x)
		if err != nil {
			return nil, fmt.Errorf("job payload is not valid JSON: %#v, error: %w", x, err)
		}
	}

	now := time.Now()
	job := &Job{
		ID:        uu.IDv4(),
		Type:      jobType,
		Payload:   payloadJSON,
		Origin:    origin,
		Priority:  priority,
		StartAt:   startAt,
		UpdatedAt: now,
		CreatedAt: now,
	}

	return job, nil
}

// NewJob creates a Job but does not add it to the queue.
// The passed payload will be marshalled to JSON or directly interpreted as JSON if possible.
// If startAt is not null then the job will not start before that time.
func NewJob(jobType, origin string, payload any, startAt nullable.Time) (*Job, error) {
	return NewJobWithPriority(jobType, origin, payload, 0, startAt)
}

// NewJobReflectType creates a Job but does not add it to the queue.
// The passed payload will be marshalled to JSON or directly interpreted as JSON if possible.
// If startAt is not null then the job will not start before that time.
//
// ReflectJobTypeOfPayload(payload) is used to
// create a job type string by using reflection on payload.
// The job type string starts with the package import path of the type
// followed by a point and the type name.
// Pointer types will be dereferenced.
func NewJobReflectType(origin string, payload any, startAt nullable.Time) (*Job, error) {
	return NewJob(ReflectJobTypeOfPayload(payload), origin, payload, startAt)
}

func Add(ctx context.Context, job *Job) error {
	return service.AddJob(ctx, job)
}

func GetJob(ctx context.Context, jobID uu.ID) (*Job, error) {
	return service.GetJob(ctx, jobID)
}

func DeleteFinishedJobs(ctx context.Context) error {
	return service.DeleteFinishedJobs(ctx)
}

// ResetJob resets the processing state of a job in the queue
// so that the job is ready to be re-processed.
func ResetJob(ctx context.Context, jobID uu.ID) error {
	return service.ResetJob(ctx, jobID)
}

// ResetJobs resets the processing state of multiple jobs in the queue
// so that they are ready to be re-processed.
func ResetJobs(ctx context.Context, jobIDs uu.IDSlice) error {
	return service.ResetJobs(ctx, jobIDs)
}

// DeleteJob deletes a job from the queue.
func DeleteJob(ctx context.Context, jobID uu.ID) error {
	return service.DeleteJob(ctx, jobID)
}

func GetAllJobsToDo(ctx context.Context) (jobs []*Job, err error) {
	return service.GetAllJobsToDo(ctx)
}

func GetAllJobsWithErrors(ctx context.Context) (jobs []*Job, err error) {
	return service.GetAllJobsWithErrors(ctx)
}
