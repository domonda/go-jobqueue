package jobworker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/domonda/go-jobqueue"
)

type JobType = string

var (
	// setupMtx guards numRunningThreads, workerWaitGroup, checkJobSignal, and stopPolling
	setupMtx sync.RWMutex

	numRunningThreads int
	workerWaitGroup   *sync.WaitGroup
	// checkJobSignal is a dummy signal notifying the thread workers that there is a new job available
	checkJobSignal chan struct{}
	// stopPolling is closed to signal the polling goroutine to stop
	stopPolling chan struct{}

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
func StartPollingAvailableJobs(interval time.Duration) error {
	if interval < 0 {
		return errors.New("polling interval cannot be negative")
	}
	if interval == 0 {
		return errors.New("polling interval cannot be zero")
	}

	setupMtx.RLock()
	stop := stopPolling
	setupMtx.RUnlock()

	if stop == nil {
		return errors.New("worker threads not started")
	}

	ticker := time.NewTicker(interval)

	go func() {
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
// The passed context does not cancel the started threads.
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

	numRunningThreads = numThreads
	workerWaitGroup = new(sync.WaitGroup)
	workerWaitGroup.Add(numThreads)
	checkJobSignal = make(chan struct{}, 1024)
	stopPolling = make(chan struct{})

	for i := range numThreads {
		go worker(i)
	}

	return nil
}

func nextJob(ctx context.Context) *jobqueue.Job {
	for {
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

	ctx := context.TODO() // We probably want to use a context to cancel the worker thread

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
	}
}

// FinishThreads waits until all worker threads have
// finished their current jobs and stops them before they start
// working on new jobs.
func FinishThreads() {
	log.Debug("Finishing threads").Log()

	setupMtx.Lock()
	defer setupMtx.Unlock()

	if numRunningThreads == 0 {
		return
	}

	// Inline stopThreadsLocked logic
	numRunningThreads = 0

	err := db.SetJobAvailableListener(context.TODO(), nil)
	if err != nil {
		OnError(err)
		log.Error("Error while setting the job available listener to nil").Err(err).Log()
	}

	close(checkJobSignal)
	// Closing stopPolling unblocks any goroutine receiving from it
	close(stopPolling)

	// Wait for workers to finish while holding the lock.
	// This is safe because workers don't acquire setupMtx.
	workerWaitGroup.Wait()
	workerWaitGroup = nil
	checkJobSignal = nil
	stopPolling = nil

	log.Info("Threads have finished").Log()
}

// StopThreads stops the threads listening for new jobs.
// Use FinishThreads to also wait for workers to complete.
func StopThreads(ctx context.Context) {
	log.Debug("Stopping threads").Log()

	setupMtx.Lock()
	defer setupMtx.Unlock()

	if numRunningThreads == 0 {
		return
	}
	numRunningThreads = 0

	err := db.SetJobAvailableListener(ctx, nil)
	if err != nil {
		OnError(err)
		log.ErrorCtx(ctx, "Error while setting the job available listener to nil").Err(err).Log()
	}

	close(checkJobSignal)
	// Closing stopPolling unblocks any goroutine receiving from it
	close(stopPolling)
}
