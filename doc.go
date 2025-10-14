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
	)

	func main() {
		ctx := context.Background()

		// Initialize the service
		service := jobworkerdb.New()
		jobqueue.SetDefaultService(service)
		jobworker.SetDataBase(service)
		defer service.Close()

		// Register a worker
		jobworker.RegisterFunc(myWorkerFunction)

		// Start worker threads
		jobworker.StartThreads(ctx, 4)
		defer jobworker.FinishThreads()

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

# Context Support

All operations support context.Context for cancellation and timeouts. The service
can also be stored in and retrieved from contexts using ContextWithService and
GetService.

# Testing

For testing, jobs can be executed synchronously without database persistence using
jobworkerdb.ContextWithSynchronousJobs, or ignored entirely using
jobworkerdb.ContextWithIgnoreJobs.

# Error Handling

All errors are wrapped using github.com/domonda/go-errs for stack traces.
Worker function errors are logged and saved to the database.
*/
package jobqueue
