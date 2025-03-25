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

	t.Run("Job ran successfully", func(t *testing.T) {
		// given

		jobResult := "OK"
		var job1 jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return jobResult, nil
		}

		jobType := "example"
		jobworker.Register(jobType, job1)

		jobID := uu.IDFrom("fd2ddce4-5d0b-4fca-aae7-30044ce8868d")
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

		waiter := &Waiter{
			Check: func() bool {
				job, err = jobqueue.GetJob(ctx, jobID)
				require.NoError(t, err)
				return job.IsFinished()
			},
			Timeout:       time.Second,
			PollFrequency: 50 * time.Millisecond,
		}

		// when
		err = waiter.Wait()
		require.NoError(t, err)

		// then
		assert.False(t, job.HasError())
		assert.Equal(t, nullable.JSON(`"`+jobResult+`"`), job.Result)
	})
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
