package jobqueue

import "fmt"

type JobDesc struct {
	Type     string
	Payload  interface{}
	Priority int64
	Origin   string
}

// String implements the fmt.Stringer interface.
// Valid to call on a nil receiver.
func (d *JobDesc) String() string {
	if d == nil {
		return "nil JobDesc"
	}
	return fmt.Sprintf("Job type %s, priority %d, origin '%s'", d.Type, d.Priority, d.Origin)
}
