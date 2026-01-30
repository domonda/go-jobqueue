package jobworker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
)

type JobType = string

var (
	// setupMtx guards numRunningThreads, workerWaitGroup, checkJobSignal, stopPolling, and workerCtx
	setupMtx sync.RWMutex

	numRunningThreads int
	workerWaitGroup   *sync.WaitGroup
	// checkJobSignal is a dummy signal notifying the thread workers that there is a new job available
	checkJobSignal chan struct{}
	// stopPolling is closed to signal the polling goroutine to stop,
	// then reassigned to a new channel for the next polling cycle
	stopPolling = make(chan struct{})
	// workerCtx is the context passed to StartThreads,
	// used to cancel worker threads when the context is cancelled
	workerCtx context.Context

	workers    = map[JobType]WorkerFunc{}
	workersMtx sync.RWMutex

	retrySchedulers    = map[JobType]ScheduleRetryFunc{}
	retrySchedulersMtx sync.RWMutex
)

func onCheckJob() {
	setupMtx.RLock()
	defer setupMtx.RUnlock()

	if numRunningThreads == 0 || checkJobSignal == nil {
		return
	}

	// Non-blocking send to avoid blocking while holding the lock.
	// If the buffer is full, there's already a pending signal.
	select {
	case checkJobSignal <- struct{}{}:
	default:
	}
}

// StartPollingAvailableJobs polls the database for available jobs every `interval` duration.
// Can be called before or after StartThreads.
func StartPollingAvailableJobs(interval time.Duration) error {
	if interval < 0 {
		return errors.New("polling interval cannot be negative")
	}
	if interval == 0 {
		return errors.New("polling interval cannot be zero")
	}

	setupMtx.RLock()
	// Capture channel reference in local variable.
	// This ensures the goroutine below will receive the close signal
	// even if stopPolling is reassigned to a new channel later.
	stop := stopPolling
	setupMtx.RUnlock()

	ticker := time.NewTicker(interval)

	go func() {
		defer errs.RecoverAndLogPanicWithFuncParams(log.ErrorWriter())

		for {
			select {
			case <-ticker.C:
				onCheckJob()
			case <-stop:
				// Receives immediately when channel is closed.
				// No value is ever sent; closing is the only signal.
				ticker.Stop()
				return
			}
		}
	}()

	return nil
}

// StartThreads starts numThreads new threads that are
// polling postgresdb for jobs to work on.
//
// The passed context is forwarded to the job worker functions
// and can be used to cancel them.
func StartThreads(ctx context.Context, numThreads int) error {
	if numThreads <= 0 {
		return errors.New("need at least 1 worker thread")
	}

	setupMtx.Lock()
	defer setupMtx.Unlock()

	if numRunningThreads > 0 {
		return errors.New("worker threads already running")
	}

	err := db.SetJobAvailableListener(ctx, onCheckJob)
	if err != nil {
		return err
	}

	workerCtx = ctx
	numRunningThreads = numThreads
	workerWaitGroup = new(sync.WaitGroup)
	workerWaitGroup.Add(numThreads)
	checkJobSignal = make(chan struct{}, 1024)

	for i := range numThreads {
		go worker(i)
	}

	return nil
}

func nextJob(ctx context.Context) *jobqueue.Job {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		job, err := db.StartNextJobOrNil(ctx)
		if err != nil {
			OnError(err)
			log.ErrorCtx(ctx, "Error while retrieving the next job").Err(err).Log()
		}
		if job != nil {
			return job
		}

		_, isOpen := <-checkJobSignal
		if !isOpen {
			return nil
		}
	}
}

func worker(threadIndex int) {
	defer workerWaitGroup.Done()

	setupMtx.RLock()
	ctx := workerCtx
	setupMtx.RUnlock()

	log, ctx := log.With().
		Int("threadIndex", threadIndex).
		SubLoggerContext(ctx)

	log.Debug("Starting the worker thread").Log()

	defer log.Debug("Worker thread ended").Log()

	for job := nextJob(ctx); job != nil; job = nextJob(ctx) {
		err := doJobAndSaveResultInDB(ctx, job)
		if err != nil {
			OnError(err)
			log.ErrorCtx(ctx, "Error while dispatching the job").
				Err(err).
				Any("job", job).
				Log()
		}
		if err := ctx.Err(); err != nil {
			return
		}
	}
}

// FinishThreads waits until all worker threads have
// finished their current jobs and stops them before they start
// working on new jobs.
//
// The passed context can be used to pass in an optional
// database connection ignoring any cancellation.
func FinishThreads(ctx context.Context) {
	log.Debug("Finishing threads").Log()

	setupMtx.Lock()
	defer setupMtx.Unlock()

	if numRunningThreads == 0 {
		return
	}

	// Inline stopThreadsLocked logic
	numRunningThreads = 0
	workerCtx = nil

	err := db.SetJobAvailableListener(context.WithoutCancel(ctx), nil)
	if err != nil {
		OnError(err)
		log.Error("Error while setting the job available listener to nil").Err(err).Log()
	}

	close(checkJobSignal)
	checkJobSignal = nil
	// Closing stopPolling unblocks any goroutine receiving from it.
	// Reassigning to a new channel is safe because running goroutines
	// have captured the old channel reference in a local variable.
	close(stopPolling)
	stopPolling = make(chan struct{})

	// Wait for workers to finish while holding the lock.
	// This is safe because workers don't acquire setupMtx.
	workerWaitGroup.Wait()
	workerWaitGroup = nil

	log.Info("Threads have finished").Log()
}

// StopThreads stops the threads listening for new jobs.
// Use FinishThreads to also wait for workers to complete.
//
// The passed context can be used to pass in an optional
// database connection ignoring any cancellation.
func StopThreads(ctx context.Context) {
	log.Debug("Stopping threads").Log()

	setupMtx.Lock()
	defer setupMtx.Unlock()

	if numRunningThreads == 0 {
		return
	}
	numRunningThreads = 0
	workerCtx = nil

	err := db.SetJobAvailableListener(context.WithoutCancel(ctx), nil)
	if err != nil {
		OnError(err)
		log.ErrorCtx(ctx, "Error while setting the job available listener to nil").Err(err).Log()
	}

	close(checkJobSignal)
	checkJobSignal = nil
	// Closing stopPolling unblocks any goroutine receiving from it.
	// Reassigning to a new channel is safe because running goroutines
	// have captured the old channel reference in a local variable.
	close(stopPolling)
	stopPolling = make(chan struct{})
}
