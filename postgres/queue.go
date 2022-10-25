package postgres

import (
	"context"
	"encoding/json"
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

type queue struct {
	serviceListeners        []jobqueue.QueueListener
	hasJobAvailableListener bool
	listenersMtx            sync.Mutex
	closed                  bool
}

///////////////////////////////////////////////////////////////////////////////
// jobqueue.Service methods

func (q *queue) SetListener(ctx context.Context, listener jobqueue.QueueListener) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, listener)

	if q.closed {
		return jobqueue.ErrClosed
	}
	if listener == nil {
		return errs.New("nil jobqueue.ServiceListener")
	}

	q.listenersMtx.Lock()
	defer q.listenersMtx.Unlock()

	if len(q.serviceListeners) == 0 {
		err = q.listen(ctx)
		if err != nil {
			return err
		}
	}

	q.serviceListeners = append(q.serviceListeners, listener)
	return nil
}

func (q *queue) listen(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	onJobStopped := func(channel, payload string) {
		defer errs.RecoverAndLogPanicWithFuncParams(log.ErrorWriter(), channel, payload)

		if q.closed {
			return
		}

		var job jobqueue.Job
		err := json.Unmarshal([]byte(payload), &job)
		if err != nil {
			log.Error("onJobStopped").Err(err).Log()
			return
		}

		q.listenersMtx.Lock()
		listeners := q.serviceListeners
		q.listenersMtx.Unlock()

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

		if q.closed {
			return
		}

		var jobBundle jobqueue.JobBundle
		err := json.Unmarshal([]byte(payload), &jobBundle)
		if err != nil {
			log.Error("onJobBundleStopped").Err(err).Log()
			return
		}

		q.listenersMtx.Lock()
		listeners := q.serviceListeners
		q.listenersMtx.Unlock()

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

func (*queue) unlisten(ctx context.Context) (err error) {
	err1 := db.Conn(ctx).UnlistenChannel("job_stopped")
	err2 := db.Conn(ctx).UnlistenChannel("job_bundle_stopped")
	return errs.Combine(err1, err2)
}

func insertJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, job)

	return db.Conn(ctx).Exec(
		`INSERT INTO worker.job
			(
				id,
				bundle_id,
				type,
				payload,
				priority,
				origin,
				start_at
			) VALUES (
				$1,
				$2,
				$3,
				$4,
				$5,
				$6,
				$7
			)`,
		job.ID,
		job.BundleID,
		job.Type,
		job.Payload,
		job.Priority,
		job.Origin,
		job.StartAt,
	)
}

func (q *queue) AddJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, job)

	if q.closed {
		return jobqueue.ErrClosed
	}

	if jobqueue.IgnoreJob(ctx, job) {
		log.Debug("Ignoring job").
			UUID("jobID", job.ID).
			Log()
		return nil
	}

	if jobqueue.SynchronousJobs(ctx) {
		log.Debug("Synchronous job").
			UUID("jobID", job.ID).
			Log()
		return jobworker.DoJob(ctx, job)
	}

	return insertJob(ctx, job)
}

func (q *queue) AddJobBundle(ctx context.Context, jobBundle *jobqueue.JobBundle) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundle)

	// Make sure jobs are initialized for bundle
	for _, job := range jobBundle.Jobs {
		job.BundleID.Set(jobBundle.ID)
	}

	if q.closed {
		return jobqueue.ErrClosed
	}

	if jobqueue.IgnoreJobBundle(ctx, jobBundle) {
		log.Debug("Ignoring job-bundle").
			UUID("jobBundleID", jobBundle.ID).
			Log()
		return nil
	}

	if jobqueue.SynchronousJobs(ctx) {
		log.Debug("Synchronous job-bundle").
			UUID("jobBundleID", jobBundle.ID).
			Log()
		for _, job := range jobBundle.Jobs {
			err = jobworker.DoJob(ctx, job)
			if err != nil {
				return err
			}
		}
		q.listenersMtx.Lock()
		listeners := q.serviceListeners
		q.listenersMtx.Unlock()
		for _, listener := range listeners {
			listener.OnJobBundleStopped(ctx, jobBundle.ID, jobBundle.Type, jobBundle.Origin)
		}
		return nil
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		err = db.Conn(ctx).Exec(
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

func (q *queue) GetStatus(ctx context.Context) (status *jobqueue.Status, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if q.closed {
		return nil, jobqueue.ErrClosed
	}

	status = new(jobqueue.Status)
	err = db.Conn(ctx).QueryRow(
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

func (q *queue) GetAllJobsToDo(ctx context.Context) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if q.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.Conn(ctx).QueryRows(
		`select *
			from worker.job
			where stopped_at is null
			order by created_at desc`,
	).ScanStructSlice(&jobs)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (q *queue) GetAllJobsWithErrors(ctx context.Context) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if q.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.Conn(ctx).QueryRows(
		`select *
			from worker.job
			where error_msg is not null
			order by stopped_at desc`,
	).ScanStructSlice(&jobs)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (q *queue) Close() (err error) {
	defer errs.WrapWithFuncParams(&err)

	if q.closed {
		return jobqueue.ErrClosed
	}

	q.closed = true

	q.listenersMtx.Lock()
	defer q.listenersMtx.Unlock()

	ctx := context.Background()

	if q.hasJobAvailableListener {
		err = db.Conn(ctx).UnlistenChannel("job_available")
		q.hasJobAvailableListener = false
	}

	if len(q.serviceListeners) > 0 {
		e := q.unlisten(ctx)
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

func (q *queue) SetJobAvailableListener(ctx context.Context, callback func()) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, callback)

	q.listenersMtx.Lock()
	defer q.listenersMtx.Unlock()

	if q.hasJobAvailableListener {
		err = db.Conn(ctx).UnlistenChannel("job_available")
		if err != nil {
			return err
		}
	}

	if callback == nil {
		q.hasJobAvailableListener = false
		return nil
	}

	q.hasJobAvailableListener = true
	return db.Conn(ctx).ListenOnChannel(
		"job_available",
		func(channel, payload string) {
			callback()
		},
		nil,
	)
}

func (q *queue) GetJob(ctx context.Context, jobID uu.ID) (job *jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if q.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.Conn(ctx).QueryRow(`select * from worker.job where id = $1`, jobID).ScanStruct(&job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (q *queue) StartNextJobOrNil(ctx context.Context) (job *jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if q.closed {
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

func (q *queue) SetJobError(ctx context.Context, jobID uu.ID, errorMsg string, errorData nullable.JSON) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, errorMsg, errorData)

	if q.closed {
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
				inner join worker.job as j on q.bundle_id = b.id
				where q.id = $1
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

func (q *queue) ResetJob(ctx context.Context, jobID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec(
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

func (q *queue) ResetJobs(ctx context.Context, jobIDs uu.IDs) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobIDs)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec(
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

// func (j *jobworkerDB) SetJobIssue(jobID uu.ID, issueType string, issueData nullable.JSON) (err error) {
// 	deerrs.WrapWithFuncParamsrror(&err, jobID, issueType, issueData)

// 	return exec(
// 		`UPDATE worker.job
// 			SET stopped_at=now(), issue_type=$1, issue_data=$2
// 			where id = $3`,
// 		issueType,
// 		issueData,
// 		jobID,
// 	)
// }

func (q *queue) SetJobResult(ctx context.Context, jobID uu.ID, result nullable.JSON) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, result)

	if q.closed {
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
				set result=$1, stopped_at=now(), updated_at=now()
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
					inner join worker.job as j on q.bundle_id = b.id
				where q.id = $1
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

func (q *queue) SetJobStart(ctx context.Context, jobID uu.ID, startAt time.Time) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, startAt)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec(
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

// func (j *jobworkerDB) GetJobResult(jobID uu.ID) (result nullable.JSON, err error) {
// 	deerrs.WrapWithFuncParamsrror(&err, jobID)

// 	if q.closed {
// 		return jobqueue.ErrClosed
// 	}

// 	conn, err := getConn()
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = conn.QueryRow(`SELECT result FROM worker.job where id = $1`).Scan(&result)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return result, nil
// }

func (q *queue) DeleteJob(ctx context.Context, jobID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec("delete from worker.job where id = $1", jobID)
}

func (q *queue) DeleteJobsFromOrigin(ctx context.Context, origin string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, origin)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec("delete from worker.job where origin = $1", origin)
}

func (q *queue) DeleteJobsOfType(ctx context.Context, jobType string) (err error) {
	defer errs.WrapWithFuncParams(&err, jobType)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec("delete from worker.job where type = $1", jobType)
}

func (q *queue) DeleteFinishedJobs(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec(
		`delete from worker.job
			where stopped_at is not null
				and	error_msg is null
				and	bundle_id is null`,
	)
}

func (q *queue) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (jobBundle *jobqueue.JobBundle, err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID)

	if q.closed {
		return nil, jobqueue.ErrClosed
	}

	err = db.TransactionReadOnly(ctx, func(ctx context.Context) error {
		err = db.Conn(ctx).QueryRow(
			`select *
				from worker.job_bundle
				where id = $1`,
			jobBundleID,
		).ScanStruct(&jobBundle)
		if err != nil {
			return err
		}

		return db.Conn(ctx).QueryRows(
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

// func (j *jobworkerDB) GetJobBundleJobs(jobBundleID uu.ID) (jobs []*jobqueue.Job, err error) {
// 	deerrs.WrapWithFuncParamsrror(&err, jobBundleID)

// 	if q.closed {
// 		return nil, jobqueue.ErrClosed
// 	}

// 	conn, err := getConn()
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = queryRowsConn(conn, `select * from worker.job where bundle_id = $1 order by created_at`, jobBundleID).ForEach(func(row rowScanner) error {
// 		job := new(jobqueue.Job)
// 		err := row.StructScan(job)
// 		if err != nil {
// 			return err
// 		}
// 		jobs = append(jobs, job)
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	return jobs, nil
// }

// func (j *jobworkerDB) CompletedJobBundleOrNil(jobBundleID uu.ID) (jobBundle *jobqueue.JobBundle, err error) {
// 	deerrs.WrapWithFuncParamsrror(&err, jobBundleID)

// 	if q.closed {
// 		return nil, jobqueue.ErrClosed
// 	}

// 	conn, err := getConn()
// 	if err != nil {
// 		return nil, err
// 	}

// 	var unfinished bool
// 	err = conn.QueryRow(`SELECT EXISTS(SELECT FROM worker.job where bundle_id = $1 AND result IS NULL)`, jobBundleID).Scan(&unfinished)
// 	if err != nil || unfinished {
// 		return nil, err
// 	}

// 	jobBundle = new(jobqueue.JobBundle)
// 	err = conn.QueryRowx(`select * from worker.job_bundle where id = $1`, jobBundleID).StructScan(jobBundle)
// 	if err != nil {
// 		return nil, err
// 	}

// 	err = queryRowsConn(conn, `select * from worker.job where bundle_id = $1 order by created_at`, jobBundleID).ForEach(func(row rowScanner) error {
// 		job := new(jobqueue.Job)
// 		err := row.StructScan(job)
// 		if err != nil {
// 			return err
// 		}
// 		jobBundle.Jobs = append(jobBundle.Jobs, job)
// 		return nil
// 	})
// 	if err != nil {
// 		return nil, err
// 	}

// 	return jobBundle, nil
// }

func (q *queue) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec("delete from worker.job_bundle where id = $1", jobBundleID)
}

// func (j *jobworkerDB) DeleteAll() (err error) {
// 	deerrs.WrapWithFuncParamsrror(&err)

// 	err = exec("delete from worker.job")
// 	if err != nil {
// 		return err
// 	}

// 	return exec("delete from worker.job_bundle")
// }

func (q *queue) DeleteJobBundlesFromOrigin(ctx context.Context, origin string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, origin)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec("delete from worker.job_bundle where origin = $1", origin)
}

func (q *queue) DeleteJobBundlesOfType(ctx context.Context, bundleType string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, bundleType)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Conn(ctx).Exec("delete from worker.job_bundle where type = $1", bundleType)
}

func (q *queue) DeleteAllJobsAndBundles(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if q.closed {
		return jobqueue.ErrClosed
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		err = db.Conn(ctx).Exec("delete from worker.job_bundle")
		if err != nil {
			return err
		}
		return db.Conn(ctx).Exec("delete from worker.job")
	})
}
