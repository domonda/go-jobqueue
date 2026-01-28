package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/caarlos0/env/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
	"github.com/domonda/go-jobqueue/jobworkerdb"
	"github.com/domonda/go-sqldb"
	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-sqldb/pqconn"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
	"github.com/domonda/golog"
)

func TestSimulateJobs(t *testing.T) {
	setupDBConn(t)

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
		job, err = jobqueue.GetJob(t.Context(), job.ID)
		require.NoError(t, err)
		assert.False(t, job.HasError())
		assert.Equal(t, nullable.JSON(`"`+jobResult+`"`), job.Result)
	})

	t.Run("Job finishes before max retry count", func(t *testing.T) {
		// given
		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return nil, nil
		}

		jobType := "c7cd81f1-61de-43c4-84f4-8c1a368e6f32"
		jobID := uu.IDFrom("125703f6-396d-447c-9286-facd91de44a7")
		retryCount := 1
		var scheduleRetry jobworker.ScheduleRetryFunc
		_, waiter := NewRegisteredJobWithWaiter(
			t,
			worker,
			jobType,
			jobID,
			retryCount,
			scheduleRetry,
		)

		// when
		err := waiter.Wait()

		// then
		require.NoError(t, err)
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
		job, err = jobqueue.GetJob(t.Context(), job.ID)
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
		job, err = jobqueue.GetJob(t.Context(), job.ID)
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
		job, err = jobqueue.GetJob(t.Context(), job.ID)
		require.NoError(t, err)
		assert.Equal(t, nullable.NonEmptyString(jobErr.Error()), job.ErrorMsg)
	})

	t.Run("Logs warning if error happens not in last retry", func(t *testing.T) {
		// given
		jobErr := errors.New("NUCLEAR_MELTDOWN")
		maxRetryCount := 1
		writer := &bytes.Buffer{}
		logConfig := golog.NewConfig(
			&golog.DefaultLevels,
			golog.AllLevelsActive,
			golog.NewJSONWriterConfig(writer, golog.NewDefaultFormat()),
		)
		jobworker.OverrideLogger(golog.NewLogger(logConfig))

		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			if job.CurrentRetryCount == 0 {
				return nil, jobErr
			}
			return nil, nil
		}

		var scheduleRetry jobworker.ScheduleRetryFunc = func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
			return time.Now(), nil
		}

		jobType := "3801a227-6e26-4040-add5-89bad4dc9b7b"
		jobID := uu.IDFrom("3653ca42-618f-4ba0-b529-d6e415627305")
		_, waiter := NewRegisteredJobWithWaiter(
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
		rawLogData := writer.String()
		logRecords := strings.SplitSeq(rawLogData, "\n")
		warningFound := false
		for logRecord := range logRecords {
			if logRecord == "" {
				continue
			}

			parsedLogRecord := &PartialLogRecord{}
			err := json.Unmarshal([]byte(logRecord), parsedLogRecord)
			require.NoError(t, err)
			assert.NotEqual(t, "ERROR", parsedLogRecord.Level)

			if parsedLogRecord.Level == "WARN" {
				warningFound = true
				assert.True(t, strings.Contains(parsedLogRecord.Message, "Job error"))
			}
		}

		assert.True(t, warningFound)
	})

	t.Run("Logs error if error happens in last retry", func(t *testing.T) {
		// given
		jobErr := errors.New("NUCLEAR_MELTDOWN")
		maxRetryCount := 1
		writer := &bytes.Buffer{}
		logConfig := golog.NewConfig(
			&golog.DefaultLevels,
			golog.AllLevelsActive,
			golog.NewJSONWriterConfig(writer, golog.NewDefaultFormat()),
		)
		jobworker.OverrideLogger(golog.NewLogger(logConfig))

		var worker jobworker.WorkerFunc = func(ctx context.Context, job *jobqueue.Job) (result any, err error) {
			return nil, jobErr
		}

		var scheduleRetry jobworker.ScheduleRetryFunc = func(ctx context.Context, job *jobqueue.Job) (time.Time, error) {
			return time.Now(), nil
		}

		jobType := "e7e4da66-c5ce-4389-be48-0d520e2c7518"
		jobID := uu.IDFrom("0de33f26-57a5-48e0-ae64-a51c218bc50e")
		_, waiter := NewRegisteredJobWithWaiter(
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
		rawLogData := writer.String()
		logRecords := strings.SplitSeq(rawLogData, "\n")
		errorFound := false
		for logRecord := range logRecords {
			if logRecord == "" {
				continue
			}

			parsedLogRecord := &PartialLogRecord{}
			err := json.Unmarshal([]byte(logRecord), parsedLogRecord)
			require.NoError(t, err)

			if parsedLogRecord.Level == "ERROR" {
				errorFound = true
				assert.True(t, strings.Contains(parsedLogRecord.Message, "Job error"))
			}
		}

		assert.True(t, errorFound)
	})
}

type PartialLogRecord struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

func NewRegisteredJobWithWaiter(
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

	err = jobqueue.Add(t.Context(), job)
	require.NoError(t, err)

	t.Cleanup(func() { jobqueue.DeleteJob(context.Background(), jobID) })

	err = jobworker.StartThreads(t.Context(), 1)
	require.NoError(t, err)

	t.Cleanup(func() { jobworker.StopThreads(context.Background()) })

	return job, NewJobWaiter(t.Context(), t, jobID)
}

func NewJobWaiter(ctx context.Context, t *testing.T, jobID uu.ID) *Waiter {
	t.Helper()

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

func setupDBConn(t *testing.T) {
	t.Helper()

	conn := pqconn.MustNew(t.Context(), dbConfigFromEnv(t))
	db.SetConn(conn)
	err := jobworkerdb.InitJobQueue(t.Context())
	require.NoError(t, err)
}

func dbConfigFromEnv(t *testing.T) *sqldb.Config {
	t.Helper()

	var config struct {
		PostgresPort     uint16 `env:"POSTGRES_PORT" envDefault:"5432"`
		PostgresHost     string `env:"POSTGRES_HOST" envDefault:"localhost"`
		PostgresUser     string `env:"POSTGRES_USER" envDefault:"postgres"`
		PostgresPassword string `env:"POSTGRES_PASSWORD"`
		PostgresDb       string `env:"POSTGRES_DB" envDefault:"domonda"`
	}
	err := env.Parse(&config)
	require.NoError(t, err)

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
