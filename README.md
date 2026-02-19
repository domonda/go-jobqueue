# go-jobqueue

Go package for a PostgreSQL-backed job queue with support for job bundles, retries, and worker pools.

## Features

- **PostgreSQL Backend**: Leverages PostgreSQL for reliable job persistence and LISTEN/NOTIFY for real-time job notifications
- **Job Bundles**: Group related jobs together and track their completion as a unit
- **Automatic Retries**: Configurable retry logic with custom scheduling functions
- **Worker Registration**: Type-safe worker registration with automatic JSON marshalling/unmarshalling
- **Flexible Priority**: Priority-based job scheduling
- **Deferred Execution**: Schedule jobs to start at a specific time
- **Context Support**: Full context.Context support throughout the API
- **Thread Pool**: Configurable worker thread pool for concurrent job processing

## Installation

```bash
go get github.com/domonda/go-jobqueue
```

## Quick Start

### 1. Set up the Database

Run the SQL schema from `schema/worker.sql` to create the necessary tables and functions:

```bash
psql -h localhost -U postgres -d your_database -f schema/worker.sql
```

### 2. Initialize the Service

```go
import (
    "context"
    "github.com/domonda/go-jobqueue"
    "github.com/domonda/go-jobqueue/jobworker"
    "github.com/domonda/go-jobqueue/jobworkerdb"
    "github.com/domonda/go-sqldb/db"
)

func main() {
    ctx := context.Background()

    // Initialize database connection
    db.SetConn(yourPostgresConnection)

    // Create and set the default job queue service
    service := jobworkerdb.New()
    jobqueue.SetDefaultService(service)
    defer service.Close()

    // Set the database for workers
    jobworker.SetDataBase(service)
}
```

### 3. Register a Worker

**Option A: Register a function with automatic reflection:**

```go
type EmailPayload struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

func sendEmail(ctx context.Context, payload *EmailPayload) error {
    // Send email logic here
    return nil
}

// Automatically uses the type name as job type
jobworker.RegisterFunc(sendEmail)
```

**Option B: Register with explicit job type:**

```go
jobworker.RegisterFuncForJobType("send-email", sendEmail)
```

**Option C: Register a WorkerFunc directly:**

```go
jobworker.Register("custom-job", func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
    // Custom job logic
    return "success", nil
})
```

### 4. Add Jobs to the Queue

```go
// Create a new job
job, err := jobqueue.NewJob(
    uu.NewID(ctx),
    "send-email",
    "user-registration",
    &EmailPayload{
        To:      "user@example.com",
        Subject: "Welcome!",
        Body:    "Thank you for registering.",
    },
    nullable.Time{}, // Start immediately
)
if err != nil {
    log.Fatal(err)
}

// Add job to queue
err = jobqueue.Add(ctx, job)
if err != nil {
    log.Fatal(err)
}
```

### 5. Start Worker Threads

```go
// Start 4 worker threads to process jobs
err := jobworker.StartThreads(ctx, 4)
if err != nil {
    log.Fatal(err)
}

// Wait for completion when shutting down
defer jobworker.FinishThreads()
```

## Advanced Usage

### Job Priorities

Jobs with higher priority values are processed first:

```go
job, err := jobqueue.NewJobWithPriority(
    uu.NewID(ctx),
    "critical-task",
    "system",
    payload,
    100, // High priority
    nullable.Time{},
)
```

### Deferred Job Execution

Schedule a job to start at a specific time:

```go
startTime := nullable.TimeFrom(time.Now().Add(1 * time.Hour))
job, err := jobqueue.NewJob(
    uu.NewID(ctx),
    "scheduled-task",
    "scheduler",
    payload,
    startTime,
)
```

### Job Retries

Configure automatic retries on failure:

```go
job, err := jobqueue.NewJob(
    uu.NewID(ctx),
    "flaky-task",
    "system",
    payload,
    nullable.Time{},
    3, // Max retry count
)

// Register a retry scheduler
jobworker.RegisterScheduleRetry("flaky-task", func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
    // Exponential backoff: 2^retryCount minutes
    delay := time.Duration(1<<uint(job.CurrentRetryCount)) * time.Minute
    return time.Now().Add(delay), nil
})
```

### Job Bundles

Group related jobs and track their completion together:

```go
jobDescriptions := []jobqueue.JobDesc{
    {Type: "process-image", Payload: imageData1, Origin: "upload"},
    {Type: "process-image", Payload: imageData2, Origin: "upload"},
    {Type: "process-image", Payload: imageData3, Origin: "upload"},
}

bundle, err := jobqueue.NewJobBundle(ctx, "image-processing", "batch-upload", jobDescriptions, nullable.Time{})
if err != nil {
    log.Fatal(err)
}

err = jobqueue.AddBundle(ctx, bundle)
if err != nil {
    log.Fatal(err)
}

// Listen for bundle completion
service.AddListener(ctx, &myListener{})
```

### Service Listeners

Listen for job and job bundle completion events:

```go
type MyListener struct{}

func (l *MyListener) OnJobStopped(ctx context.Context, jobID uu.ID, jobType, origin string, willRetry bool) {
    log.Printf("Job %s completed", jobID)
}

func (l *MyListener) OnJobBundleStopped(ctx context.Context, bundleID uu.ID, bundleType, origin string) {
    log.Printf("Job bundle %s completed", bundleID)
}

// Register listener
service := jobqueue.GetService(ctx)
service.AddListener(ctx, &MyListener{})
```

### Polling for Jobs

For environments where LISTEN/NOTIFY might not work reliably, use polling:

```go
// Poll for new jobs every 5 seconds
err := jobworker.StartPollingAvailableJobs(5 * time.Second)
if err != nil {
    log.Fatal(err)
}
```

### Queue Management

```go
// Get queue status
status, err := jobqueue.GetStatus(ctx)
fmt.Printf("Jobs: %d, Bundles: %d\n", status.NumJobs, status.NumJobBundles)

// Get all pending jobs
jobs, err := jobqueue.GetAllJobsToDo(ctx)

// Get jobs with errors
errorJobs, err := jobqueue.GetAllJobsWithErrors(ctx)

// Reset a failed job to retry
err = jobqueue.ResetJob(ctx, jobID)

// Delete finished jobs
err = jobqueue.DeleteFinishedJobs(ctx)

// Delete specific job
err = jobqueue.DeleteJob(ctx, jobID)
```

### Context-Specific Services

Use different job queue services in different contexts:

```go
// Create a custom service instance
customService := jobworkerdb.New()

// Add it to context
ctx = jobqueue.ContextWithService(ctx, customService)

// Jobs added in this context use the custom service
jobqueue.Add(ctx, job)
```

### Synchronous Job Execution for Testing

Execute jobs synchronously without database persistence:

```go
import "github.com/domonda/go-jobqueue/jobworkerdb"

ctx = jobworkerdb.ContextWithSynchronousJobs(ctx)

// This job will execute immediately in the current goroutine
jobqueue.Add(ctx, job)
```

### Ignoring Jobs for Testing

Ignore job execution entirely:

```go
ctx = jobworkerdb.ContextWithIgnoreJobs(ctx)

// Jobs added in this context are discarded
jobqueue.Add(ctx, job)
```

## Architecture

### Components

- **jobqueue**: Core package with job/bundle types and service interface
- **jobworker**: Worker registration, execution logic, and thread pool management
- **jobworkerdb**: PostgreSQL implementation of the job queue service

### Database Schema

The package uses three main tables in the `worker` schema:

- `worker.job`: Individual jobs with type, payload, priority, and status
- `worker.job_bundle`: Job bundles grouping multiple jobs
- Database triggers: Automatic PostgreSQL NOTIFY on job availability and completion

### Job Lifecycle

1. **Created**: Job is inserted into `worker.job` table
2. **Available**: Trigger fires `job_available` notification (if start_at is reached)
3. **Started**: Worker picks up job, sets `started_at` timestamp
4. **Processing**: Worker function executes
5. **Completed**: Result or error is saved, `stopped_at` timestamp set
6. **Notification**: Trigger fires `job_stopped` notification

## Error Handling

All errors are wrapped using `github.com/domonda/go-errs` for enhanced stack traces. Worker functions can return errors which will be:

- Logged with full context
- Saved to the database with `error_msg` and optional `error_data`
- Retried if retry count hasn't been exceeded
- Surfaced through service listeners

## Best Practices

1. **Idempotent Workers**: Design workers to be safely retried
2. **Job Origins**: Use meaningful origin strings for debugging and filtering
3. **Timeouts**: Always pass contexts with appropriate timeouts to worker functions
4. **Graceful Shutdown**: Call `jobworker.FinishThreads()` to let in-flight jobs complete
5. **Monitor Queue Depth**: Regularly check queue status to detect bottlenecks
6. **Clean Up**: Periodically delete finished jobs to prevent table bloat

## Testing

Run the test suite:

```bash
# Start PostgreSQL via Docker Compose
docker-compose up -d

# Run tests
go test ./...
```

## Dependencies

- PostgreSQL 12+
- [github.com/domonda/go-sqldb](https://github.com/domonda/go-sqldb) - SQL database utilities
- [github.com/domonda/go-types](https://github.com/domonda/go-types) - Type system extensions
- [github.com/domonda/go-errs](https://github.com/domonda/go-errs) - Enhanced error handling
- [github.com/domonda/golog](https://github.com/domonda/golog) - Structured logging

## License

See LICENSE file
