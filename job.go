package jobqueue

import (
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

	Type              string        `db:"type"     json:"type"` // CHECK(length("type") > 0 AND length("type") <= 100)
	Payload           notnull.JSON  `db:"payload"  json:"payload"`
	Priority          int64         `db:"priority" json:"priority"`
	Origin            string        `db:"origin"   json:"origin"` // CHECK(length(origin) > 0 AND length(origin) <= 100)
	MaxRetryCount     int           `db:"max_retry_count"   json:"maxRetryCount"`
	CurrentRetryCount int           `db:"current_retry_count"   json:"currentRetryCount"`
	StartAt           nullable.Time `db:"start_at" json:"startAt"` // If not NULL, earliest time to start the job

	StartedAt nullable.Time `db:"started_at" json:"startedAt"` // Time when started working on the job, or NULL when not started
	StoppedAt nullable.Time `db:"stopped_at" json:"stoppedAt"` // Time when working on job was stoped because of a decision question or an error, or NULL

	ErrorMsg  nullable.NonEmptyString `db:"error_msg"  json:"errorMsg"`  // If there was an error working off the job
	ErrorData nullable.JSON           `db:"error_data" json:"errorData"` // Optional error metadata
	Result    nullable.JSON           `db:"result"     json:"result"`    // Result if the job returned one

	UpdatedAt time.Time `db:"updated_at" json:"updatedAt"`
	CreatedAt time.Time `db:"created_at" json:"createdAt"`
}

func (j *Job) Started() bool {
	return j.StartedAt.IsNotNull()
}

func (j *Job) Stopped() bool {
	return j.StoppedAt.IsNotNull()
}

func (j *Job) IsFinished() bool {
	if !j.Stopped() {
		return false
	}
	return j.CurrentRetryCount >= j.MaxRetryCount || !j.Result.IsNull()
}

func (j *Job) Succeeded() bool {
	return j.IsFinished() && !j.HasError()
}

// HasError returns true if the receiver is not nil
// and has an ErrorMsg.
// Valid to call on a nil receiver.
func (j *Job) HasError() bool {
	return j.ErrorMsg.IsNotNull()
}

// String implements the fmt.Stringer interface.
// Valid to call on a nil receiver.
func (j *Job) String() string {
	return fmt.Sprintf("Job %s, type %s, priority %d, created at %s from origin '%s' max retry count %d current retry count %d", j.ID, j.Type, j.Priority, j.CreatedAt, j.Origin, j.MaxRetryCount, j.CurrentRetryCount)
}

// NewJobWithPriority creates a Job but does not add it to the queue.
// The passed payload will be marshalled to JSON or directly interpreted as JSON if possible.
// If startAt is not null then the job will not start before that time.
func NewJobWithPriority(
	id uu.ID,
	jobType,
	origin string,
	payload any,
	priority int64,
	startAt nullable.Time,
	retryCount ...int,
) (*Job, error) {
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
		ID:        id,
		Type:      jobType,
		Payload:   payloadJSON,
		Origin:    origin,
		Priority:  priority,
		StartAt:   startAt,
		UpdatedAt: now,
		CreatedAt: now,
	}

	if len(retryCount) > 0 {
		job.MaxRetryCount = retryCount[0]
	}

	return job, nil
}

// NewJob creates a Job but does not add it to the queue.
// The passed payload will be marshalled to JSON or directly interpreted as JSON if possible.
// If startAt is not null then the job will not start before that time.
func NewJob(
	id uu.ID,
	jobType,
	origin string,
	payload any,
	startAt nullable.Time,
	retryCount ...int,
) (*Job, error) {
	return NewJobWithPriority(id, jobType, origin, payload, 0, startAt, retryCount...)
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
func NewJobReflectType(id uu.ID, origin string, payload any, startAt nullable.Time) (*Job, error) {
	return NewJob(id, ReflectJobTypeOfPayload(payload), origin, payload, startAt)
}
