package jobqueue

import (
	"context"
	"fmt"
)

type Status struct {
	NumJobs       int
	NumJobBundles int
	// NumWorkerThreads int
}

// IsZero returns true if the receiver is nil
// or dereferenced equal to its zero value.
// Valid to call on a nil receiver.
func (s *Status) IsZero() bool {
	return s == nil || *s == Status{}
}

// String implements the fmt.Stringer interface.
// Valid to call on a nil receiver.
func (s *Status) String() string {
	if s == nil {
		return "nil Status"
	}
	// return fmt.Sprintf("Status{NumJobs: %d, NumJobBundles: %d, NumWorkerThreads: %d}", s.NumJobs, s.NumJobBundles, s.NumWorkerThreads)
	return fmt.Sprintf("Status{NumJobs: %d, NumJobBundles: %d}", s.NumJobs, s.NumJobBundles)
}

func GetStatus(ctx context.Context) (status *Status, err error) {
	return service.GetStatus(ctx)
}
