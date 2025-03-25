package jobworker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/domonda/go-jobqueue"
)

var (
	// numRunningThreads represents the number of currently running threads
	numRunningThreads int

	workerWaitGroup *sync.WaitGroup
	workers         = make(map[string]WorkerFunc)
	workersMtx      sync.RWMutex

	// checkJobSignal is a dummy signal notifying the thread workers that there is a new job available
	checkJobSignal chan struct{}
)

func onCheckJob() {
	if numRunningThreads > 0 && checkJobSignal != nil {
		checkJobSignal <- struct{}{}
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

	ticker := time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-ticker.C:
				onCheckJob()
			case _, isOpen := <-checkJobSignal:
				// if the checkJob channel is closed, exit
				if !isOpen {
					ticker.Stop()
					return
				}
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
	checkJobSignal = make(chan struct{}, 256)

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

	log.Debug("starting the worker thread").Log()

	defer log.Debug("worker thread ended").Log()

	for job := nextJob(ctx); job != nil; job = nextJob(ctx) {
		err := doJobAndSaveResultInDB(ctx, job)
		if err != nil {
			OnError(err)
			log.ErrorCtx(ctx, "error while dispatching the job").
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
	log.Debug("finishing threads").Log()

	if numRunningThreads == 0 {
		return
	}

	StopThreads(context.TODO())

	workerWaitGroup.Wait()
	workerWaitGroup = nil
	checkJobSignal = nil

	log.Info("threads have finished").Log()
}

// StopThreads stops the threads listening for new jobs.
func StopThreads(ctx context.Context) {
	log.Debug("stopping threads").Log()

	if numRunningThreads == 0 {
		return
	}

	numRunningThreads = 0

	err := db.SetJobAvailableListener(ctx, nil)
	if err != nil {
		OnError(err)
		log.ErrorCtx(ctx, "error while setting the job available listener").Err(err).Log()
	}

	close(checkJobSignal)
}
