/*
Package jobworker provides worker registration, execution logic, and thread pool
management for the jobqueue package.

# Overview

The jobworker package handles the execution side of the job queue. It manages:
- Worker function registration
- Thread pool for concurrent job processing
- Job execution with error handling
- Retry scheduling logic

# Worker Registration

Workers can be registered in three ways:

1. Automatic reflection-based registration:

	func processEmail(ctx context.Context, payload *EmailPayload) error {
		// Process email
		return nil
	}
	jobworker.RegisterFunc(processEmail)

2. Explicit job type registration:

	jobworker.RegisterFuncForJobType("send-email", processEmail)

3. Direct WorkerFunc registration:

	jobworker.Register("custom-job", func(ctx context.Context, job *jobqueue.Job) (any, error) {
		// Custom logic
		return result, nil
	})

# Worker Functions

Worker functions can have various signatures:

	// With context and error result
	func(ctx context.Context, payload *MyType) error

	// With context and typed result
	func(ctx context.Context, payload *MyType) (*Result, error)

	// Without context
	func(payload *MyType) error

The payload type is automatically unmarshalled from the job's JSON payload.

# Thread Pool

Start a worker thread pool to process jobs concurrently:

	err := jobworker.StartThreads(ctx, 4) // Start 4 worker threads
	if err != nil {
		log.Fatal(err)
	}
	defer jobworker.FinishThreads() // Wait for jobs to complete

# Retry Scheduling

Register retry schedulers to control retry timing:

	jobworker.RegisterScheduleRetry("flaky-job", func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
		// Exponential backoff
		delay := time.Duration(1<<uint(job.CurrentRetryCount)) * time.Minute
		return time.Now().Add(delay), nil
	})

# Job Execution

Jobs are executed by calling DoJob, which:
1. Looks up the registered worker for the job type
2. Executes the worker function with the job payload
3. Handles panics and converts them to errors
4. Sets job.Result or job.ErrorMsg based on the outcome
5. Logs execution details

# Polling

For environments where PostgreSQL LISTEN/NOTIFY isn't reliable, use polling:

	err := jobworker.StartPollingAvailableJobs(5 * time.Second)

# Database Interface

The worker package requires a DataBase implementation to be set:

	jobworker.SetDataBase(service)

This interface handles job persistence and queue operations.
*/
package jobworker
