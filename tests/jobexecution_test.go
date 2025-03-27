package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DAtek/env"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
	"github.com/domonda/go-jobqueue/jobworkerdb"
	"github.com/domonda/go-sqldb"
	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-sqldb/pqconn"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimulateJobs(t *testing.T) {
	ctx := context.Background()
	setupDBConn(ctx, t)

	t.Run("Job result is being stored on success", func(t *testing.T) {
		// given
		jobResult := "OK"
		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return jobResult, nil
		}

		jobType := "8005f973-fa09-4159-8fcc-ad166a006c40"
		jobID := uu.IDFrom("ac86612f-c93b-4589-843f-ab16065b668b")
		retryCount := 0
		var scheduleRetry jobworker.ScheduleRetryFunc
		job, waiter := NewRegisteredJobWithWaiter(
			ctx,
			t,
			worker,
			jobType,
			jobID,
			retryCount,
			scheduleRetry,
		)

		// when
		err := waiter.Wait()
		require.NoError(t, err)

		// then
		job, err = jobqueue.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.False(t, job.HasError())
		assert.Equal(t, nullable.JSON(`"`+jobResult+`"`), job.Result)
	})

	t.Run("Job error is being stored on failure", func(t *testing.T) {
		// given
		jobErr := errors.New("NUCLEAR_MELTDOWN")
		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return nil, jobErr
		}

		jobType := "45f0684d-2993-4065-8dd2-f7abf1763ef0"
		jobID := uu.IDFrom("4ac2792b-a12c-42b5-9a75-60ff316da013")
		retryCount := 0
		var scheduleRetry jobworker.ScheduleRetryFunc
		job, waiter := NewRegisteredJobWithWaiter(
			ctx,
			t,
			worker,
			jobType,
			jobID,
			retryCount,
			scheduleRetry,
		)

		// when
		err := waiter.Wait()
		require.NoError(t, err)

		// then
		job, err = jobqueue.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.True(t, job.HasError())
		assert.Equal(t, nullable.NonEmptyString(jobErr.Error()), job.ErrorMsg)
	})

	t.Run("Job will be retried on failure when retry is configured", func(t *testing.T) {
		// given
		jobErr := errors.New("NUCLEAR_MELTDOWN")
		maxRetryCount := 3

		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			if job.CurrentRetryCount >= maxRetryCount {
				return nil, nil
			}
			return nil, jobErr
		}

		var scheduleRetry jobworker.ScheduleRetryFunc = func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
			return time.Now(), nil
		}

		jobType := "7476c878-eec4-4d0c-9ff5-fea0f3dcaf1c"
		jobID := uu.IDFrom("54b3c462-6d9b-45cb-8096-2d2f262881e1")
		job, waiter := NewRegisteredJobWithWaiter(
			ctx,
			t,
			worker,
			jobType,
			jobID,
			maxRetryCount,
			scheduleRetry,
		)

		// when
		err := waiter.Wait()
		require.NoError(t, err)

		// then
		job, err = jobqueue.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.False(t, job.HasError())
	})

	t.Run("Job fails if all attempts fail", func(t *testing.T) {
		// given
		jobErr := errors.New("NUCLEAR_MELTDOWN")
		maxRetryCount := 3

		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return nil, jobErr
		}

		var scheduleRetry jobworker.ScheduleRetryFunc = func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
			return time.Now(), nil
		}

		jobType := "3af846f5-bacb-4922-b5e5-8156005bcef2"
		jobID := uu.IDFrom("8f4ee5f3-6aa6-4a4a-88b5-5c874a901428")
		job, waiter := NewRegisteredJobWithWaiter(
			ctx,
			t,
			worker,
			jobType,
			jobID,
			maxRetryCount,
			scheduleRetry,
		)

		// when
		err := waiter.Wait()
		require.NoError(t, err)

		// then
		job, err = jobqueue.GetJob(ctx, job.ID)
		require.NoError(t, err)
		assert.Equal(t, nullable.NonEmptyString(jobErr.Error()), job.ErrorMsg)
	})
}

func NewRegisteredJobWithWaiter(
	ctx context.Context,
	t *testing.T,
	workerFunc jobworker.WorkerFunc,
	jobType string,
	jobID uu.ID,
	retryCount int,
	scheduleRetry jobworker.ScheduleRetryFunc,
) (
	*jobqueue.Job,
	*Waiter,
) {
	t.Helper()
	jobworker.Register(jobType, workerFunc)

	if scheduleRetry != nil {
		jobworker.RegisterScheduleRetry(jobType, scheduleRetry)
	}

	job, err := jobqueue.NewJob(
		jobID,
		jobType,
		"origin",
		"{}",
		nullable.TimeNow(),
		retryCount,
	)
	require.NoError(t, err)

	err = jobqueue.Add(ctx, job)
	require.NoError(t, err)

	t.Cleanup(func() { jobqueue.DeleteJob(ctx, jobID) })

	err = jobworker.StartThreads(ctx, 1)
	require.NoError(t, err)

	t.Cleanup(func() { jobworker.StopThreads(ctx) })

	return job, NewJobWaiter(ctx, t, jobID)
}

func NewJobWaiter(ctx context.Context, t *testing.T, jobID uu.ID) *Waiter {
	return &Waiter{
		Check: func() bool {
			job, err := jobqueue.GetJob(ctx, jobID)
			require.NoError(t, err)
			return job.IsFinished()
		},
		Timeout:       time.Second,
		PollFrequency: 50 * time.Millisecond,
	}
}

type Waiter struct {
	Check         func() bool
	Timeout       time.Duration
	PollFrequency time.Duration
}

func (w *Waiter) Wait() error {
	start := time.Now()
	for {
		if time.Since(start) > w.Timeout {
			return errors.New("TIMEOUT")
		}

		if w.Check() {
			return nil
		}
		time.Sleep(w.PollFrequency)
	}
}

func setupDBConn(ctx context.Context, t *testing.T) {
	t.Helper()
	conn := pqconn.MustNew(ctx, dbConfigFromEnv())
	db.SetConn(conn)
	err := jobworkerdb.InitJobQueue(ctx)
	require.NoError(t, err)
}

func dbConfigFromEnv() *sqldb.Config {
	config, err := loadEnv()
	if err != nil {
		panic(err)
	}

	return &sqldb.Config{
		Driver:   "postgres",
		User:     config.PostgresUser,
		Password: config.PostgresPassword,
		Host:     config.PostgresHost,
		Port:     config.PostgresPort,
		Database: config.PostgresDb,
		Extra:    map[string]string{"sslmode": "disable"},
	}
}

type DBEnvConfig struct {
	PostgresPort     uint16
	PostgresHost     string
	PostgresUser     string
	PostgresPassword string
	PostgresDb       string
}

var loadEnv = env.NewLoader[DBEnvConfig]()
