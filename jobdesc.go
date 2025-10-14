package jobqueue

import "fmt"

// JobDesc describes a job to be created, typically used when creating job bundles.
// It contains the essential information needed to create a Job instance.
type JobDesc struct {
	// Type is the job type string. If empty, ReflectJobTypeOfPayload(Payload) will be used.
	Type string
	// Payload is the job data that will be marshalled to JSON.
	Payload any
	// Priority determines job processing order. Higher values are processed first.
	Priority int64
	// Origin identifies the source or context that created the job.
	Origin string
}

// String implements the fmt.Stringer interface.
// Valid to call on a nil receiver.
func (d *JobDesc) String() string {
	if d == nil {
		return "nil JobDesc"
	}
	return fmt.Sprintf("Job type %s, priority %d, origin '%s'", d.Type, d.Priority, d.Origin)
}
