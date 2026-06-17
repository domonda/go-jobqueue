/*
Package jobqueue provides a PostgreSQL-backed job queue with support for job bundles,
retries, and worker pools.

# Overview

The jobqueue package allows you to queue jobs for asynchronous processing with
PostgreSQL as the backend. It supports job priorities, scheduled execution,
automatic retries, and grouping related jobs into bundles.

# Basic Usage

First, initialize the service and register workers:

	import (
		"context"
		"github.com/domonda/go-jobqueue"
		"github.com/domonda/go-jobqueue/jobworker"
		"github.com/domonda/go-jobqueue/jobworkerdb"
		"github.com/domonda/go-sqldb/db"
	)

	func main() {
		ctx := context.Background()

		// Set up the database connection, then initialize the job queue.
		// InitJobQueue creates the DB-backed service and registers it as the
		// default for both the jobqueue and jobworker packages.
		db.SetConn(postgresConnection)
		if err := jobworkerdb.InitJobQueue(ctx); err != nil {
			log.Fatal(err)
		}
		defer jobqueue.Close()

		// Register a worker
		jobworker.RegisterFunc(myWorkerFunction)

		// Start worker threads
		jobworker.StartThreads(ctx, 4)
		defer jobworker.FinishThreads(ctx)

		// Add jobs
		job, _ := jobqueue.NewJob(uu.NewID(ctx), "my-job", "origin", payload, nullable.Time{})
		jobqueue.Add(ctx, job)
	}

# Job Types

Jobs are defined by a type string and a JSON payload. Workers are registered
for specific job types and process jobs with matching types.

# Job Lifecycle

1. Created - Job is inserted into the database
2. Available - PostgreSQL NOTIFY alerts workers
3. Started - Worker picks up job and sets started_at
4. Processing - Worker function executes
5. Completed - Result or error is saved, stopped_at set
6. Notification - PostgreSQL NOTIFY alerts listeners

# Job Priorities

Jobs with higher priority values are processed first. Default priority is 0.

# Scheduled Jobs

Jobs can be scheduled to start at a specific time using the startAt parameter.
Jobs won't be processed until the scheduled time is reached.

# Retries

Jobs can be configured with a maximum retry count. If a job fails and hasn't
exceeded the retry count, a retry scheduler function determines when to retry.
Register retry schedulers using jobworker.RegisterScheduleRetry.

# Job Bundles

Related jobs can be grouped into bundles. The bundle tracks completion of all
jobs and provides a single notification when all jobs are finished.

# Multiple Worker Processes

Worker pools can run in multiple processes concurrently against the same
PostgreSQL database; this is the primary way to scale processing horizontally.
Each process calls jobworker.StartThreads independently and they all compete for
the same queue.

Every job is claimed atomically with SELECT ... FOR UPDATE SKIP LOCKED, so no two
worker processes ever pick up the same available job at once. (A job may still
run more than once over its lifetime — a failed job is retried, and a job
abandoned by a crashed worker is reclaimed — but never concurrently.) While a
worker runs a job it periodically updates the job's worker_alive_at heartbeat
(see jobworker.HeartbeatInterval), which lets other processes tell whether that
worker is still alive or has crashed.

Jobs abandoned by a crashed worker are reclaimed by
jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, deadFor), which only resets
jobs whose worker is provably dead: its heartbeat stale by more than deadFor, or,
for a job that failed but was not yet rescheduled, stopped more than deadFor ago.
As long as deadFor is at least 3 × HeartbeatInterval, a live worker stays ahead
of that window — its heartbeat keeps advancing for as long as it owns the job,
including while its retry scheduler runs — so this is safe to run on every
process's startup even while other processes are actively working. Plain
jobworkerdb.InitJobQueue performs no reset.

In-progress crash recovery requires heartbeats to be enabled. With
jobworker.HeartbeatInterval set to 0 a claimed job's worker_alive_at stays NULL,
so the reaper cannot distinguish a crashed worker from a slow one and never
reclaims a started-but-not-stopped job (only jobs already marked errored with
retries remaining). The previous InitJobQueueResetDanglingJobs, which reset every
started-but-not-stopped job on startup, has been removed as unsafe with multiple
worker processes; heartbeats-off single-process setups must keep heartbeats
enabled or reset abandoned jobs themselves.

# Context Support

All operations support context.Context for cancellation and timeouts. The service
can also be stored in and retrieved from contexts using ContextWithService and
GetService.

# Testing

For testing, jobs can be executed synchronously without database persistence using
jobworkerdb.ContextWithSynchronousJobs, or ignored entirely using
jobworkerdb.ContextWithIgnoreJob.

# Error Handling

All errors are wrapped using github.com/domonda/go-errs for stack traces.
Worker function errors are logged and saved to the database.
*/
package jobqueue
