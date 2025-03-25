package jobworker

import (
	"context"
	"fmt"
	"time"

	"github.com/domonda/go-jobqueue"
)

type ScheduleRetryFunc func(ctx context.Context, job *jobqueue.Job) (nextStart time.Time, err error)

func RegisterScheduleRetry(jobType JobType, strategy ScheduleRetryFunc) {
	retrySchedulersMtx.Lock()
	defer retrySchedulersMtx.Unlock()

	if _, exists := retrySchedulers[jobType]; exists {
		panic(fmt.Errorf("retry scheduler for job type '%s' already exists", jobType))
	}

	retrySchedulers[jobType] = strategy
}
