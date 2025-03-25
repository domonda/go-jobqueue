package jobworkerdb_test

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

func TestJobExecution(t *testing.T) {
	ctx := context.Background()
	setupDBConn(ctx, t)

	t.Run("Job stores result on success", func(t *testing.T) {
		// given
		jobResult := "OK"
		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return jobResult, nil
		}

		jobType := "8005f973-fa09-4159-8fcc-ad166a006c40"
		jobID := uu.IDFrom("ac86612f-c93b-4589-843f-ab16065b668b")
		job, waiter := NewRegisteredJobWithWaiter(
			ctx,
			t,
			worker,
			jobType,
			jobID,
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

	t.Run("Job stores error on failure", func(t *testing.T) {
		// given
		jobErr := errors.New("NUCLEAR_MELTDOWN")
		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return nil, jobErr
		}

		jobType := "45f0684d-2993-4065-8dd2-f7abf1763ef0"
		jobID := uu.IDFrom("4ac2792b-a12c-42b5-9a75-60ff316da013")
		job, waiter := NewRegisteredJobWithWaiter(
			ctx,
			t,
			worker,
			jobType,
			jobID,
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
}

func NewRegisteredJobWithWaiter(
	ctx context.Context,
	t *testing.T,
	workerFunc jobworker.WorkerFunc,
	jobType string,
	jobID uu.ID,
) (
	*jobqueue.Job,
	*Waiter,
) {
	t.Helper()
	jobworker.Register(jobType, workerFunc)

	job, err := jobqueue.NewJob(
		jobID,
		jobType,
		"origin",
		"{}",
		nullable.TimeNow(),
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
			return job.IsStopped()
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
