package jobworkerdb

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
	"github.com/domonda/go-sqldb"
	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
)

type jobworkerDB struct {
	serviceListeners        []jobqueue.ServiceListener
	hasJobAvailableListener bool
	listenersMtx            sync.Mutex
	closed                  bool
}

///////////////////////////////////////////////////////////////////////////////
// jobqueue.Service methods

func (j *jobworkerDB) AddListener(ctx context.Context, listener jobqueue.ServiceListener) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, listener)

	if j.closed {
		return jobqueue.ErrClosed
	}
	if listener == nil {
		return errs.New("<nil> jobqueue.ServiceListener")
	}

	j.listenersMtx.Lock()
	defer j.listenersMtx.Unlock()

	if len(j.serviceListeners) == 0 {
		err = j.listen(ctx)
		if err != nil {
			return err
		}
	}

	j.serviceListeners = append(j.serviceListeners, listener)
	return nil
}

func (j *jobworkerDB) listen(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	onJobStopped := func(channel, payload string) {
		defer errs.RecoverAndLogPanicWithFuncParams(log.ErrorWriter(), channel, payload)

		if j.closed {
			return
		}

		var job jobqueue.Job
		err := json.Unmarshal([]byte(payload), &job)
		if err != nil {
			log.ErrorCtx(ctx, "onJobStopped").Err(err).Log()
			return
		}

		j.listenersMtx.Lock()
		listeners := j.serviceListeners
		j.listenersMtx.Unlock()

		ctx := context.Background() // Don't use ctx of enclosing listen method

		for _, l := range listeners {
			l.OnJobStopped(ctx, job.ID, job.Type, job.Origin)
		}
	}

	err = db.Conn(ctx).ListenOnChannel("job_stopped", onJobStopped, nil)
	if err != nil {
		return err
	}

	onJobBundleStopped := func(channel, payload string) {
		defer errs.RecoverAndLogPanicWithFuncParams(log.ErrorWriter(), channel, payload)

		if j.closed {
			return
		}

		var jobBundle jobqueue.JobBundle
		err := json.Unmarshal([]byte(payload), &jobBundle)
		if err != nil {
			log.ErrorCtx(ctx, "onJobBundleStopped").Err(err).Log()
			return
		}

		j.listenersMtx.Lock()
		listeners := j.serviceListeners
		j.listenersMtx.Unlock()

		ctx := context.Background() // Don't use ctx of enclosing listen method

		for _, l := range listeners {
			l.OnJobBundleStopped(ctx, jobBundle.ID, jobBundle.Type, jobBundle.Origin)
		}
	}

	err = db.Conn(ctx).ListenOnChannel("job_bundle_stopped", onJobBundleStopped, nil)
	if err != nil {
		return err
	}

	return nil
}

func (*jobworkerDB) unlisten(ctx context.Context) (err error) {
	err1 := db.Conn(ctx).UnlistenChannel("job_stopped")
	err2 := db.Conn(ctx).UnlistenChannel("job_bundle_stopped")
	return errors.Join(err1, err2)
}

func insertJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, job)

	return db.Exec(ctx,
		`INSERT INTO worker.job
			(
				id,
				bundle_id,
				type,
				payload,
				priority,
				origin,
				max_retry_count,
				start_at
			) VALUES (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6,
				$7,
				$8
			)`,
		job.ID,
		job.BundleID,
		job.Type,
		job.Payload,
		job.Priority,
		job.Origin,
		job.MaxRetryCount,
		job.StartAt,
	)
}

func (j *jobworkerDB) AddJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, job)

	if j.closed {
		return jobqueue.ErrClosed
	}

	if IgnoreJob(ctx, job) {
		log.Debug("Ignoring job").
			UUID("jobID", job.ID).
			Log()
		return nil
	}

	if SynchronousJobs(ctx) {
		log.Debug("Synchronous job").
			UUID("jobID", job.ID).
			Log()
		return jobworker.DoJob(ctx, job)
	}

	return insertJob(ctx, job)
}

func (j *jobworkerDB) AddJobBundle(ctx context.Context, jobBundle *jobqueue.JobBundle) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundle)

	// Make sure jobs are initialized for bundle
	for _, job := range jobBundle.Jobs {
		job.BundleID.Set(jobBundle.ID)
	}

	if j.closed {
		return jobqueue.ErrClosed
	}

	if IgnoreJobBundle(ctx, jobBundle) {
		log.Debug("Ignoring job-bundle").
			UUID("jobBundleID", jobBundle.ID).
			Log()
		return nil
	}

	if SynchronousJobs(ctx) {
		log.Debug("Synchronous job-bundle").
			UUID("jobBundleID", jobBundle.ID).
			Log()
		for _, job := range jobBundle.Jobs {
			err = jobworker.DoJob(ctx, job)
			if err != nil {
				return err
			}
		}
		j.listenersMtx.Lock()
		listeners := j.serviceListeners
		j.listenersMtx.Unlock()
		for _, listener := range listeners {
			listener.OnJobBundleStopped(ctx, jobBundle.ID, jobBundle.Type, jobBundle.Origin)
		}
		return nil
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		err = db.Exec(ctx,
			`insert into worker.job_bundle (id, type, origin, num_jobs)
				values ($1, $2, $3, $4)`,
			jobBundle.ID,
			jobBundle.Type,
			jobBundle.Origin,
			jobBundle.NumJobs,
		)
		if err != nil {
			return err
		}

		for _, job := range jobBundle.Jobs {
			err = insertJob(ctx, job)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (j *jobworkerDB) GetStatus(ctx context.Context) (status *jobqueue.Status, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed {
		return nil, jobqueue.ErrClosed
	}

	status = new(jobqueue.Status)
	err = db.QueryRow(ctx,
		`select
			(select count(*) from worker.job)        as num_jobs,
			(select count(*) from worker.job_bundle) as num_job_bundles`,
	).Scan(
		&status.NumJobs,
		&status.NumJobBundles,
	)
	if err != nil {
		return nil, err
	}
	return status, nil
}

func (j *jobworkerDB) GetAllJobsToDo(ctx context.Context) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.QueryRows(ctx,
		`select *
			from worker.job
			where stopped_at is null
			order by start_at nulls first, created_at`,
	).ScanStructSlice(&jobs)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (j *jobworkerDB) GetAllJobsStartedBefore(ctx context.Context, before time.Time) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.QueryRows(ctx,
		`select *
			from worker.job
			where started_at is not null
				and started_at < $1
				and stopped_at is null
			order by started_at`,
		before,
	).ScanStructSlice(&jobs)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (j *jobworkerDB) GetAllJobsWithErrors(ctx context.Context) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.QueryRows(ctx,
		`select *
			from worker.job
			where error_msg is not null
			order by stopped_at`,
	).ScanStructSlice(&jobs)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (j *jobworkerDB) Close() (err error) {
	defer errs.WrapWithFuncParams(&err)

	if j.closed {
		return jobqueue.ErrClosed
	}

	j.closed = true

	j.listenersMtx.Lock()
	defer j.listenersMtx.Unlock()

	ctx := context.Background()

	if j.hasJobAvailableListener {
		err = db.Conn(ctx).UnlistenChannel("job_available")
		j.hasJobAvailableListener = false
	}

	if len(j.serviceListeners) > 0 {
		e := j.unlisten(ctx)
		if e != nil {
			if err == nil {
				err = errs.Errorf("error %w from db.unlisten", e)
			} else {
				err = errs.Errorf("error %s from db.unlisten after error: %w", e, err)
			}
		}
	}

	return err
}

///////////////////////////////////////////////////////////////////////////////
// jobqueue.DB methods

func (j *jobworkerDB) SetJobAvailableListener(ctx context.Context, callback func()) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, callback)

	j.listenersMtx.Lock()
	defer j.listenersMtx.Unlock()

	if j.hasJobAvailableListener {
		err = db.Conn(ctx).UnlistenChannel("job_available")
		if err != nil {
			return err
		}
	}

	if callback == nil {
		j.hasJobAvailableListener = false
		return nil
	}

	j.hasJobAvailableListener = true
	return db.Conn(ctx).ListenOnChannel(
		"job_available",
		func(channel, payload string) {
			callback()
		},
		nil,
	)
}

func (j *jobworkerDB) GetJob(ctx context.Context, jobID uu.ID) (job *jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if j.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.QueryRow(ctx, `select * from worker.job where id = $1`, jobID).ScanStruct(&job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (j *jobworkerDB) StartNextJobOrNil(ctx context.Context) (job *jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed {
		return nil, jobqueue.ErrClosed
	}

	jobTypes := jobworker.RegisteredJobTypes()

	err = db.Transaction(ctx, func(ctx context.Context) error {
		tx := db.Conn(ctx)
		now := time.Now()

		err = tx.QueryRow(
			`select *
				from worker.job
				where started_at is null
					and (start_at is null or start_at <= $1)
					and "type" = any($2::text[])
				order by
					priority desc,
					created_at desc
				limit 1
				for update skip locked`,
			now,
			jobTypes,
		).ScanStruct(&job)
		if err != nil {
			return sqldb.ReplaceErrNoRows(err, nil)
		}

		job.StartedAt.Set(now)
		job.UpdatedAt = now
		return tx.Exec(
			`update worker.job
				set started_at=$1, updated_at=$2
				where id = $3`,
			job.StartedAt,
			job.UpdatedAt,
			job.ID,
		)
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (j *jobworkerDB) SetJobError(ctx context.Context, jobID uu.ID, errorMsg string, errorData nullable.JSON) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, errorMsg, errorData)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		tx := db.Conn(ctx)
		// update job
		err = tx.Exec(
			`update worker.job
				set stopped_at=now(), error_msg=$1, error_data=$2, updated_at=now()
				where id = $3`,
			errorMsg,
			errorData,
			jobID,
		)
		if err != nil {
			return err
		}

		// update job bundle
		var jobBundleID uu.ID
		err = tx.QueryRow(
			`select b.id
				from worker.job_bundle as b
				inner join worker.job as j on j.bundle_id = b.id
				where j.id = $1
				for update skip locked`,
			jobID,
		).Scan(&jobBundleID)
		if sqldb.ReplaceErrNoRows(err, nil) != nil {
			return err
		}

		if jobBundleID.Valid() {
			err = tx.Exec(
				`update worker.job_bundle
					set num_jobs_stopped=num_jobs_stopped+1, updated_at=now()
					where id = $1`,
				jobBundleID,
			)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (j *jobworkerDB) ResetJob(ctx context.Context, jobID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		`update worker.job
			set
				started_at=null,
				stopped_at=null,
				error_msg=null,
				error_data=null,
				result=null,
				updated_at=now()
			where id = $1`,
		jobID,
	)
}

func (j *jobworkerDB) ResetJobs(ctx context.Context, jobIDs uu.IDs) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobIDs)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		`update worker.job
			set
				started_at=null,
				stopped_at=null,
				error_msg=null,
				error_data=null,
				result=null,
				updated_at=now()
			where id = any($1)`,
		jobIDs,
	)
}

func (j *jobworkerDB) SetJobResult(ctx context.Context, jobID uu.ID, result nullable.JSON) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, result)

	if j.closed {
		return jobqueue.ErrClosed
	}

	// if the result is `nil`, set an empty object so that the bundle knows the job existed correctly
	if len(result) == 0 {
		result = []byte("{}")
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		tx := db.Conn(ctx)

		err = tx.Exec(
			`update worker.job
				set result=$1, stopped_at=now(), updated_at=now(), error_msg=null, error_data=null
				where id = $2`,
			result,
			jobID,
		)
		if err != nil {
			return err
		}

		var jobBundleID uu.ID
		err = tx.QueryRow(
			`select b.id
				from worker.job_bundle as b
					inner join worker.job as j on j.bundle_id = b.id
				where j.id = $1
				for update skip locked`,
			jobID,
		).Scan(&jobBundleID)
		if sqldb.ReplaceErrNoRows(err, nil) != nil {
			return err
		}

		if jobBundleID.Valid() {
			err = tx.Exec(
				`update worker.job_bundle
					set num_jobs_stopped=num_jobs_stopped+1, updated_at=now()
					where id = $1`,
				jobBundleID,
			)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (j *jobworkerDB) SetJobStart(ctx context.Context, jobID uu.ID, startAt time.Time) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, startAt)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		`update worker.job
			set
				start_at=$1,
				started_at=null,
				stopped_at=null,
				error_msg=null,
				error_data=null,
				updated_at=now()
			where id = $2`,
		startAt,
		jobID,
	)
}

func (j *jobworkerDB) ScheduleRetry(ctx context.Context, jobID uu.ID, startAt time.Time, retryCount int) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, startAt)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		`update worker.job
			set
				start_at=$1,
				started_at=null,
				stopped_at=null,
				current_retry_count=$2,
				updated_at=now()
			where id = $3`,
		startAt,
		retryCount,
		jobID,
	)
}

func (j *jobworkerDB) DeleteJob(ctx context.Context, jobID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx, "delete from worker.job where id = $1", jobID)
}

func (j *jobworkerDB) DeleteJobsFromOrigin(ctx context.Context, origin string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, origin)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx, "delete from worker.job where origin = $1", origin)
}

func (j *jobworkerDB) DeleteJobsOfType(ctx context.Context, jobType string) (err error) {
	defer errs.WrapWithFuncParams(&err, jobType)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx, "delete from worker.job where type = $1", jobType)
}

func (j *jobworkerDB) DeleteFinishedJobs(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		`delete from worker.job
			where stopped_at is not null
				and	error_msg is null
				and	bundle_id is null`,
	)
}

func (j *jobworkerDB) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (jobBundle *jobqueue.JobBundle, err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID)

	if j.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.TransactionReadOnly(ctx, func(ctx context.Context) error {
		err = db.QueryRow(ctx,
			`select *
				from worker.job_bundle
				where id = $1`,
			jobBundleID,
		).ScanStruct(&jobBundle)
		if err != nil {
			return err
		}

		return db.QueryRows(ctx,
			`select *
				from worker.job
				where bundle_id = $1
				order by created_at`,
			jobBundleID,
		).ScanStructSlice(&jobBundle.Jobs)
	})
	if err != nil {
		return nil, err
	}

	return jobBundle, nil
}

func (j *jobworkerDB) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx, "delete from worker.job_bundle where id = $1", jobBundleID)
}

func (j *jobworkerDB) DeleteJobBundlesFromOrigin(ctx context.Context, origin string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, origin)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx, "delete from worker.job_bundle where origin = $1", origin)
}

func (j *jobworkerDB) DeleteJobBundlesOfType(ctx context.Context, bundleType string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, bundleType)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx, "delete from worker.job_bundle where type = $1", bundleType)
}

func (j *jobworkerDB) DeleteAllJobsAndBundles(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed {
		return jobqueue.ErrClosed
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		err = db.Exec(ctx, "delete from worker.job_bundle")
		if err != nil {
			return err
		}
		return db.Exec(ctx, "delete from worker.job")
	})
}
