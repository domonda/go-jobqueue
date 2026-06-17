package jobqueue

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/notnull"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
)

// Job is an in-memory snapshot of a worker.job row as it was read from the
// database. Its predicate methods (Started, Stopped, StartedAndNotStopped,
// IsFinished, Succeeded, HasError, WorkerAlive) evaluate this snapshot and do
// NOT re-query the database for the current state — a concurrently running
// worker (in this or another process) may have moved the job on since it was
// loaded.
//
// In particular, WorkerAlive compares WorkerAliveAt against the local process
// clock (time.Since) on the snapshot; it performs no database access, so its
// result depends on the caller's clock agreeing with the database clock that
// wrote WorkerAliveAt. For an authoritative, skew-immune liveness decision use
// the database-side reaper jobworkerdb.InitJobQueueResetInterruptedJobs, which
// compares the stored timestamps against the database now().
type Job struct {
	db.TableName `db:"worker.job"`

	ID       uu.ID         `db:"id,primarykey" json:"id"`
	BundleID uu.NullableID `db:"bundle_id"     json:"bundleId"`

	Type              string        `db:"type"     json:"type"` // CHECK(length("type") > 0 AND length("type") <= 100)
	Payload           notnull.JSON  `db:"payload"  json:"payload"`
	Priority          int64         `db:"priority" json:"priority"`
	Origin            string        `db:"origin"   json:"origin"` // CHECK(length(origin) > 0 AND length(origin) <= 100)
	MaxRetryCount     int           `db:"max_retry_count"   json:"maxRetryCount"`
	CurrentRetryCount int           `db:"current_retry_count"   json:"currentRetryCount"`
	StartAt           nullable.Time `db:"start_at" json:"startAt"` // If not NULL, earliest time to start the job

	StartedAt     nullable.Time `db:"started_at"      json:"startedAt"`     // Time when started working on the job, or NULL when not started
	WorkerAliveAt nullable.Time `db:"worker_alive_at" json:"workerAliveAt"` // Heartbeat updated periodically while a worker processes the job, NULL when not being processed. A stale value while StoppedAt is NULL indicates the worker crashed.
	StoppedAt     nullable.Time `db:"stopped_at"      json:"stoppedAt"`     // Time when working on job was stoped because of a decision question or an error, or NULL

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

// StartedAndNotStopped returns true if a worker has claimed the job
// and is working on it (started but not yet stopped).
// May be stale after worker process has crashed.
func (j *Job) StartedAndNotStopped() bool {
	return j.Started() && !j.Stopped()
}

// WorkerAlive reports whether the worker processing this job is considered
// alive, based on deadFor as the maximum allowed age of the last heartbeat
// (WorkerAliveAt). It returns false if the job is not currently being
// processed. A job that is being processed but whose last heartbeat is older
// than deadFor was most likely abandoned by a crashed worker.
//
// The age is computed from this Job's in-memory WorkerAliveAt field against the
// local process clock (time.Since), not from a live database query. Because
// WorkerAliveAt was written with the database clock, the result is only accurate
// to the extent the calling process's clock matches the database's; under clock
// skew it can be off. For a skew-immune decision use the database-side reaper
// (jobworkerdb.InitJobQueueResetInterruptedJobs), which compares against now().
func (j *Job) WorkerAlive(deadFor time.Duration) bool {
	if !j.Started() || j.Stopped() || j.WorkerAliveAt.IsNull() {
		return false
	}
	return time.Since(j.WorkerAliveAt.Get()) <= deadFor
}

func (j *Job) IsFinished() bool {
	if !j.Stopped() {
		return false
	}
	return j.CurrentRetryCount >= j.MaxRetryCount || j.ErrorMsg.IsNull()
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
