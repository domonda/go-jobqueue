package jobworker

import (
	"context"
	"fmt"
	"time"

	"github.com/domonda/go-jobqueue"
)

// ScheduleRetryFunc computes the next start time for a failed job that is going
// to be retried. It is registered per job type with RegisterScheduleRetry.
type ScheduleRetryFunc func(ctx context.Context, job *jobqueue.Job) (nextStart time.Time, err error)

// RegisterScheduleRetry registers a ScheduleRetryFunc for the given job type.
// It panics if a retry scheduler has already been registered for that type.
func RegisterScheduleRetry(jobType JobType, scheduleFunc ScheduleRetryFunc) {
	retrySchedulersMtx.Lock()
	defer retrySchedulersMtx.Unlock()

	if _, exists := retrySchedulers[jobType]; exists {
		panic(fmt.Errorf("retry scheduler for job type '%s' already exists", jobType))
	}

	retrySchedulers[jobType] = scheduleFunc
}
