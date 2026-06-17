package jobworker

import (
	"context"
	"sync"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-types/uu"
)

// startJobHeartbeat starts a goroutine that periodically updates the
// worker_alive_at timestamp of the job with jobID in the database every
// HeartbeatInterval. This lets observers tell whether the worker processing the
// job is still alive: while the worker runs, worker_alive_at keeps advancing; if
// the worker process crashes, the goroutine dies with it and worker_alive_at
// stops advancing, so a stale worker_alive_at indicates a job abandoned by a
// crashed worker.
//
// The returned stop function stops the heartbeat and blocks until the
// goroutine has terminated, guaranteeing that no further worker_alive_at update
// happens after it returns. It must be called when the job has finished
// processing, before the job is marked as stopped, so the heartbeat is
// synchronized with the job's completion. stop is idempotent and safe to call
// more than once, so callers can both call it explicitly and defer it as a
// leak-safety net.
//
// If HeartbeatInterval is <= 0 heartbeating is disabled and stop is a no-op.
func startJobHeartbeat(ctx context.Context, jobID uu.ID) (stop func()) {
	if HeartbeatInterval <= 0 {
		return func() {}
	}

	// Detach from ctx cancellation so that a shutdown or job-timeout
	// cancellation does not stop the heartbeat while the worker function is
	// still running. The heartbeat must reflect whether the worker process is
	// alive, not whether ctx is still active. The goroutine's lifetime is
	// controlled solely by the returned stop function.
	ctx = context.WithoutCancel(ctx)
	// A cancellable child of the detached context lets stop() abort an in-flight
	// heartbeat write immediately instead of blocking on the database call (e.g.
	// a hung connection) until it returns on its own.
	ctx, cancel := context.WithCancel(ctx)

	stopChan := make(chan struct{})
	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)
		defer errs.RecoverAndLogPanicWithFuncParams(log.ErrorWriter(), jobID)

		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopChan:
				return
			case <-ticker.C:
				// Bound each write so a hung database connection cannot freeze the
				// heartbeat (and, via stop(), the worker thread) indefinitely; the
				// next tick retries. ctx is also cancelled by stop(), so an
				// in-flight write is aborted promptly on shutdown.
				writeCtx, cancelWrite := context.WithTimeout(ctx, HeartbeatInterval)
				err := db.SetJobWorkerAlive(writeCtx, jobID)
				cancelWrite()
				// Skip logging when ctx was cancelled by stop(): that error is from
				// our own shutdown, not a real heartbeat failure.
				if err != nil && ctx.Err() == nil {
					OnError(err)
					log.ErrorCtx(ctx, "Error while updating the job heartbeat").
						UUID("jobID", jobID).
						Err(err).
						Log()
				}
			}
		}
	}()

	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			close(stopChan)
			cancel() // abort any in-flight heartbeat write
			<-doneChan
		})
	}
}
