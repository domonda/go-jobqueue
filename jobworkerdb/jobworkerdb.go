package jobworkerdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/domonda/go-errs"
	"github.com/domonda/go-sqldb"
	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
)

type jobworkerDB struct {
	serviceListeners        []jobqueue.ServiceListener
	hasJobAvailableListener bool
	listenersMtx            sync.Mutex
	closed                  atomic.Bool
}

///////////////////////////////////////////////////////////////////////////////
// jobqueue.Service methods

func (j *jobworkerDB) AddListener(ctx context.Context, listener jobqueue.ServiceListener) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, listener)

	if j.closed.Load() {
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

		if j.closed.Load() {
			return
		}

		var notification struct {
			ID        uu.ID  `json:"id"`
			Type      string `json:"type"`
			Origin    string `json:"origin"`
			WillRetry bool   `json:"willRetry"`
		}
		err := json.Unmarshal([]byte(payload), &notification)
		if err != nil {
			log.ErrorCtx(ctx, "onJobStopped").Err(err).Log()
			return
		}

		j.listenersMtx.Lock()
		listeners := j.serviceListeners
		j.listenersMtx.Unlock()

		ctx := context.Background() // Don't use ctx of enclosing listen method

		for _, l := range listeners {
			l.OnJobStopped(ctx, notification.ID, notification.Type, notification.Origin, notification.WillRetry)
		}
	}

	err = db.ListenOnChannel(ctx, "job_stopped", onJobStopped, nil)
	if err != nil {
		return err
	}

	onJobBundleStopped := func(channel, payload string) {
		defer errs.RecoverAndLogPanicWithFuncParams(log.ErrorWriter(), channel, payload)

		if j.closed.Load() {
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

	err = db.ListenOnChannel(ctx, "job_bundle_stopped", onJobBundleStopped, nil)
	if err != nil {
		return err
	}

	return nil
}

func (*jobworkerDB) unlisten(ctx context.Context) (err error) {
	err1 := db.UnlistenChannel(ctx, "job_stopped")
	err2 := db.UnlistenChannel(ctx, "job_bundle_stopped")
	return errors.Join(err1, err2)
}

func insertJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, job)

	return db.Exec(ctx,
		/*sql*/ `
			INSERT INTO worker.job
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
			)
		`,
		job.ID,            // $1
		job.BundleID,      // $2
		job.Type,          // $3
		job.Payload,       // $4
		job.Priority,      // $5
		job.Origin,        // $6
		job.MaxRetryCount, // $7
		job.StartAt,       // $8
	)
}

func (j *jobworkerDB) AddJob(ctx context.Context, job *jobqueue.Job) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, job)

	if j.closed.Load() {
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

	if j.closed.Load() {
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
			/*sql*/ `
				insert into worker.job_bundle (id, type, origin, num_jobs)
				values ($1, $2, $3, $4)
			`,
			jobBundle.ID,      // $1
			jobBundle.Type,    // $2
			jobBundle.Origin,  // $3
			jobBundle.NumJobs, // $4
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

	if j.closed.Load() {
		return nil, jobqueue.ErrClosed
	}

	status = new(jobqueue.Status)
	status.NumJobs, status.NumJobBundles, err = db.QueryRowAs2[int, int](ctx,
		/*sql*/ `
			select
			(select count(*) from worker.job)        as num_jobs,
			(select count(*) from worker.job_bundle) as num_job_bundles
		`,
	)
	if err != nil {
		return nil, err
	}
	return status, nil
}

func (j *jobworkerDB) GetAllJobsToDo(ctx context.Context) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed.Load() {
		return nil, jobqueue.ErrClosed
	}

	return db.QueryRowsAsSlice[*jobqueue.Job](ctx,
		/*sql*/ `
			select *
			from worker.job
			where stopped_at is null
			order by start_at nulls first, created_at
		`,
	)
}

func (j *jobworkerDB) GetAllJobsStartedBefore(ctx context.Context, before time.Time) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed.Load() {
		return nil, jobqueue.ErrClosed
	}

	return db.QueryRowsAsSlice[*jobqueue.Job](ctx,
		/*sql*/ `
			select *
			from worker.job
			where started_at is not null
				and started_at < $1
				and stopped_at is null
			order by started_at
		`,
		before, // $1
	)
}

func (j *jobworkerDB) GetAllJobsWithErrors(ctx context.Context) (jobs []*jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed.Load() {
		return nil, jobqueue.ErrClosed
	}

	return db.QueryRowsAsSlice[*jobqueue.Job](ctx,
		/*sql*/ `
			select *
			from worker.job
			where error_msg is not null
			order by stopped_at
		`,
	)
}

func (j *jobworkerDB) Close() (err error) {
	defer errs.WrapWithFuncParams(&err)

	if !j.closed.CompareAndSwap(false, true) {
		return jobqueue.ErrClosed
	}

	j.listenersMtx.Lock()
	defer j.listenersMtx.Unlock()

	ctx := context.Background()

	if j.hasJobAvailableListener {
		err = db.UnlistenChannel(ctx, "job_available")
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

	// Release the cached prepared statements (claim + heartbeat). The closed flag
	// is already set above, so no new claim or heartbeat will start using them.
	err = errors.Join(err, closeCachedStmts())

	return err
}

///////////////////////////////////////////////////////////////////////////////
// jobqueue.DB methods

func (j *jobworkerDB) SetJobAvailableListener(ctx context.Context, callback func()) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, callback)

	j.listenersMtx.Lock()
	defer j.listenersMtx.Unlock()

	if j.hasJobAvailableListener {
		err = db.UnlistenChannel(ctx, "job_available")
		if err != nil {
			return err
		}
	}

	if callback == nil {
		j.hasJobAvailableListener = false
		return nil
	}

	j.hasJobAvailableListener = true
	return db.ListenOnChannel(ctx,
		"job_available",
		func(channel, payload string) {
			callback()
		},
		nil,
	)
}

func (j *jobworkerDB) GetJob(ctx context.Context, jobID uu.ID) (job *jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if j.closed.Load() {
		return nil, jobqueue.ErrClosed
	}

	return db.QueryRowAs[*jobqueue.Job](ctx,
		/*sql*/ `select * from worker.job where id = $1`, jobID,
	)
}

// buildClaimJobQuery assembles the StartNextJobOrNil claim statement: a single
// CTE that selects the next claimable job (FOR UPDATE SKIP LOCKED) and marks it
// started, atomically and without any query parameters, returning the updated
// row. Being parameterless lets it be a cached prepared statement.
//
// The registered job types are inlined as a `"type" in ('a','b')` predicate; the
// set is constant per process (it only changes via jobworker.Register/Unregister,
// normally at startup) and is SQL-injection checked at registration, so inlining
// the types as literals lets PostgreSQL plan against the actual values instead of
// an opaque `= any($n)` array parameter.
//
// All timestamps use the database clock (now()), so started_at / worker_alive_at /
// updated_at are immune to clock skew between worker processes and the database.
// worker_alive_at is stamped with now() only when heartbeats are enabled; with
// heartbeats disabled it is left NULL so the reaper's heartbeat-staleness branch
// (which requires worker_alive_at IS NOT NULL) stays inert and never resets a
// still-running job that has no liveness signal. HeartbeatInterval is process
// startup config, so that choice is inlined here too — no query parameter.
//
// jobTypes must be non-empty; StartNextJobOrNil returns early for the empty case
// (nothing to claim) so this never builds an invalid empty `in ()`.
func buildClaimJobQuery(jobTypes []string, conn sqldb.QueryFormatter) string {
	workerAliveAt := "null"
	if jobworker.HeartbeatInterval > 0 {
		workerAliveAt = "now()"
	}

	// FormatStringLiteral returns a complete, properly quoted PostgreSQL string
	// literal (single quotes, '' escaping, E'' for backslashes). Job types are
	// also SQL-injection checked in jobworker.Register.
	typeLiterals := make([]string, len(jobTypes))
	for i, jobType := range jobTypes {
		typeLiterals[i] = conn.FormatStringLiteral(jobType)
	}

	// The CTE `claimed` finds and row-locks the single next job to run; the outer
	// UPDATE marks that same row as started. Both run as one statement, so the
	// claim is atomic without a surrounding transaction.
	return fmt.Sprintf(
		/*sql*/ `
			with claimed as (
				select id
				from worker.job
				where started_at is null                          -- not started yet
					and (start_at is null or start_at <= now())   -- scheduled start reached (or unscheduled)
					and "type" in (%s)                            -- only job types this process has workers for
				order by
					priority desc,    -- highest priority first,
					created_at asc    -- then oldest first (FIFO within a priority)
				limit 1
				-- skip locked: take the next row not already locked by another worker
				-- (rather than blocking on it), so concurrent workers each claim a
				-- different job.
				for update skip locked
			)
			update worker.job
			set started_at      = now(),
				worker_alive_at = %s,  -- liveness anchor: now() if heartbeats enabled, else null
				updated_at      = now()
			from claimed
			where worker.job.id = claimed.id      -- the row the CTE locked
			returning worker.job.*                -- full updated row, scanned into the job struct
		`,
		strings.Join(typeLiterals, ","), // for "type" in (%s)
		workerAliveAt,                   // for worker_alive_at = %s
	)
}

// claimJobStmt cache, guarded by claimJobStmtMtx. The claim statement takes no
// parameters, so it is prepared once and reused; it is re-prepared only when the
// registered job types change (the jobworker generation), which is startup-only.
//
// This cache is package-level, not per jobworkerDB instance: a process runs a
// single active service (see InitJobQueue), and closeCachedStmts releases it on
// Close. Running two services against different connections concurrently is not
// supported, as they would share this one cached statement.
var (
	claimJobStmtMtx   sync.Mutex
	claimJobStmtGen   uint64
	claimJobStmtQuery func(ctx context.Context, args ...any) (*jobqueue.Job, error)
	claimJobStmtClose func() error
)

// claimJobStmt returns the cached prepared claim statement for the given
// registered job types, (re)preparing it when the generation changes. jobTypes
// and gen come from a single jobworker.RegisteredJobTypes call, so they are a
// consistent pair. The returned query func wraps a pool-safe *sql.Stmt
// (database/sql re-prepares it per pooled connection) and is safe to call
// concurrently, so callers execute it outside the lock. The generation changes
// only on Register/Unregister (startup-only), so the re-prepare — and the Close of
// the previously prepared statement — does not race a concurrent claim.
func claimJobStmt(ctx context.Context, jobTypes []string, gen uint64) (func(context.Context, ...any) (*jobqueue.Job, error), error) {
	claimJobStmtMtx.Lock()
	defer claimJobStmtMtx.Unlock()

	if claimJobStmtQuery == nil || gen != claimJobStmtGen {
		if claimJobStmtClose != nil {
			// Clear the cache before closing the old statement so that, even if
			// Close fails, we never leave a stale (closed-or-close-failed)
			// statement cached at the old generation. The next call then rebuilds
			// from scratch instead of closing the same statement again.
			closeOld := claimJobStmtClose
			claimJobStmtQuery, claimJobStmtClose = nil, nil
			if err := closeOld(); err != nil {
				return nil, err
			}
		}
		query := buildClaimJobQuery(jobTypes, db.Conn(ctx))
		queryFunc, closeStmt, err := db.QueryRowAsStmt[*jobqueue.Job](ctx, query)
		if err != nil {
			claimJobStmtQuery, claimJobStmtClose = nil, nil
			return nil, err
		}
		claimJobStmtQuery, claimJobStmtClose, claimJobStmtGen = queryFunc, closeStmt, gen
	}
	return claimJobStmtQuery, nil
}

func (j *jobworkerDB) StartNextJobOrNil(ctx context.Context) (job *jobqueue.Job, err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed.Load() {
		return nil, jobqueue.ErrClosed
	}

	jobTypes, gen := jobworker.RegisteredJobTypes()
	if len(jobTypes) == 0 {
		// No registered worker types: nothing this process can claim.
		return nil, nil
	}

	claimJob, err := claimJobStmt(ctx, jobTypes, gen)
	if err != nil {
		return nil, err
	}

	// The claim statement is a single CTE that selects the next job
	// (FOR UPDATE SKIP LOCKED) and marks it started atomically, so no explicit
	// transaction is needed. `skip locked` lets workers compete: a row already
	// locked by another worker is skipped rather than waited on. It takes no
	// arguments (now() and inlined job types), so it runs as a prepared statement.
	job, err = claimJob(ctx)
	if err != nil {
		return nil, sqldb.ReplaceErrNoRows(err, nil)
	}
	return job, nil
}

func (j *jobworkerDB) SetJobError(ctx context.Context, jobID uu.ID, errorMsg string, errorData nullable.JSON) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, errorMsg, errorData)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		// SetJobError records a TERMINAL failure: the job has stopped and will
		// not be retried. current_retry_count is clamped up to max_retry_count so
		// the stopped+errored row is unambiguously terminal.
		//
		// This clamp is load-bearing for the startup reaper
		// (resetInterruptedRetryableJobs): the reaper treats a stopped, errored
		// job with current_retry_count < max_retry_count as a crash leftover (a
		// worker that died between SetJobError and ScheduleRetry on an older,
		// pre-heartbeat version) and resets it. A worker on this version only
		// calls SetJobError once it is done retrying — retries exhausted, or a
		// missing/failing retry scheduler (see doJobAndSaveResultInDB) — so
		// clamping keeps the reaper's retries-remaining branch reserved for
		// genuine rolling-upgrade leftovers. Without it, a job with a missing
		// retry scheduler would be reset and re-run on every startup forever and
		// its bundle would never complete (it would never be counted below).
		err = db.Exec(ctx,
			/*sql*/ `
				update worker.job
				set stopped_at=now(),
					error_msg=$1,
					error_data=$2,
					current_retry_count=max_retry_count,
					worker_alive_at=null,
					updated_at=now()
				where id = $3
			`,
			errorMsg,  // $1
			errorData, // $2
			jobID,     // $3
		)
		if err != nil {
			return err
		}

		// Count the job in its bundle. SetJobError is always terminal (the clamp
		// above guarantees current_retry_count >= max_retry_count), so a bundled
		// job is counted exactly once here; ResetJob/ResetJobs decrement it again
		// if the job is later reset. `for update` (blocking, not skip locked) is
		// used because every completion must update the one shared bundle row.
		jobBundleID, err := db.QueryRowAsOr(ctx,
			uu.IDNull,
			/*sql*/ `
				select b.id
				from worker.job_bundle as b
				inner join worker.job as j on j.bundle_id = b.id
				where j.id = $1
				for update
			`,
			jobID, // $1
		)
		if err != nil {
			return err
		}
		if jobBundleID.IsNull() {
			return nil
		}

		return db.Exec(ctx,
			/*sql*/ `
				update worker.job_bundle
				set num_jobs_stopped=num_jobs_stopped+1, updated_at=now()
				where id = $1
			`,
			jobBundleID.Get(), // $1
		)
	})
}

func (j *jobworkerDB) ResetJob(ctx context.Context, jobID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		// Decrement the bundle counter if this job was already counted
		// as stopped. A job is counted when SetJobResult or SetJobError
		// incremented num_jobs_stopped, which happens when:
		// - the job succeeded (error_msg IS NULL, result set), or
		// - the job failed with all retries exhausted (current_retry_count >= max_retry_count).
		// Without this decrement, resetting and re-running the job would
		// increment the counter a second time, violating the
		// CHECK(num_jobs_stopped <= num_jobs) constraint.
		err = db.Exec(ctx,
			/*sql*/ `
				update worker.job_bundle
				set num_jobs_stopped = num_jobs_stopped - 1, updated_at = now()
				where id = (
					select j.bundle_id
					from worker.job as j
					where j.id = $1
						and j.bundle_id is not null
						and j.stopped_at is not null
						and (j.error_msg is null or j.current_retry_count >= j.max_retry_count)
				)
			`,
			jobID, // $1
		)
		if err != nil {
			return err
		}

		return db.Exec(ctx,
			/*sql*/ `
				update worker.job
				set
					started_at=null,
					stopped_at=null,
					error_msg=null,
					error_data=null,
					result=null,
					worker_alive_at=null,
					current_retry_count=0,
					updated_at=now()
				where id = $1
			`,
			jobID, // $1
		)
	})
}

func (j *jobworkerDB) ResetJobs(ctx context.Context, jobIDs uu.IDs) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobIDs)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		// Decrement bundle counters for jobs that were already counted
		// as stopped. See ResetJob for the detailed explanation.
		// Jobs may belong to different bundles, so group by bundle_id
		// and decrement each bundle's counter by the number of its
		// already-counted jobs being reset.
		err = db.Exec(ctx,
			/*sql*/ `
				update worker.job_bundle as b
				set num_jobs_stopped = b.num_jobs_stopped - counted.cnt, updated_at = now()
				from (
					select j.bundle_id, count(*) as cnt
					from worker.job as j
					where j.id = any($1)
						and j.bundle_id is not null
						and j.stopped_at is not null
						and (j.error_msg is null or j.current_retry_count >= j.max_retry_count)
					group by j.bundle_id
				) as counted
				where b.id = counted.bundle_id
			`,
			jobIDs, // $1
		)
		if err != nil {
			return err
		}

		return db.Exec(ctx,
			/*sql*/ `
				update worker.job
				set
					started_at=null,
					stopped_at=null,
					error_msg=null,
					error_data=null,
					result=null,
					worker_alive_at=null,
					current_retry_count=0,
					updated_at=now()
				where id = any($1)
			`,
			jobIDs, // $1
		)
	})
}

func (j *jobworkerDB) SetJobResult(ctx context.Context, jobID uu.ID, result nullable.JSON) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, result)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	// if the result is `nil`, set an empty object so that the bundle knows the job existed correctly
	if len(result) == 0 {
		result = []byte("{}")
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		err = db.Exec(ctx,
			/*sql*/ `
				update worker.job
				set result=$1,
					stopped_at=now(),
					worker_alive_at=null,
					updated_at=now(),
					error_msg=null,
					error_data=null
				where id = $2
			`,
			result, // $1
			jobID,  // $2
		)
		if err != nil {
			return err
		}

		// Use `for update` (blocking) instead of `for update skip locked`
		// because every job completion must increment the bundle counter.
		// This is different from StartNextJobOrNil where `skip locked`
		// is correct because workers compete for any unclaimed job row.
		// Here there is one specific bundle row that must be updated
		// by every completing job, so skipping would lose increments
		// and potentially leave the bundle stuck forever.
		jobBundleID, err := db.QueryRowAsOr(ctx,
			uu.IDNull,
			/*sql*/ `
				select b.id
				from worker.job_bundle as b
					inner join worker.job as j on j.bundle_id = b.id
				where j.id = $1
				for update
			`,
			jobID, // $1
		)
		if err != nil {
			return err
		}

		if jobBundleID.IsNotNull() {
			err = db.Exec(ctx,
				/*sql*/ `
					update worker.job_bundle
					set num_jobs_stopped=num_jobs_stopped+1, updated_at=now()
					where id = $1
				`,
				jobBundleID.Get(), // $1
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

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `
			update worker.job
			set
				start_at=$1,
				started_at=null,
				stopped_at=null,
				error_msg=null,
				error_data=null,
				worker_alive_at=null,
				updated_at=now()
			where id = $2
		`,
		startAt, // $1
		jobID,   // $2
	)
}

// setJobWorkerAliveStmt cache, guarded by setJobWorkerAliveMtx. The heartbeat
// query is constant — only the job id varies, as a bind parameter — so it is
// prepared once and reused for the process lifetime.
var (
	setJobWorkerAliveMtx   sync.Mutex
	setJobWorkerAliveExec  func(ctx context.Context, args ...any) error
	setJobWorkerAliveClose func() error
)

// setJobWorkerAliveStmt returns the cached prepared heartbeat statement,
// preparing it on first use. The returned exec func wraps a pool-safe *sql.Stmt
// (database/sql re-prepares it per pooled connection) and is safe to call
// concurrently, as the heartbeat fires from one goroutine per in-flight job. A
// failed prepare leaves the cache nil so the next heartbeat retries. The
// statement is released by closeCachedStmts on jobworkerDB.Close.
func setJobWorkerAliveStmt(ctx context.Context) (func(context.Context, ...any) error, error) {
	setJobWorkerAliveMtx.Lock()
	defer setJobWorkerAliveMtx.Unlock()

	if setJobWorkerAliveExec == nil {
		// Only update worker_alive_at while the job is actually being processed
		// (claimed but not yet stopped). The guard prevents a heartbeat that races
		// with job completion from resurrecting worker_alive_at on a stopped job.
		// updated_at is bumped together so every UPDATE of worker.job advances it.
		execFunc, closeStmt, err := db.ExecStmt(ctx,
			/*sql*/ `
				update worker.job
				set worker_alive_at=now(), updated_at=now()
				where id = $1
					and started_at is not null
					and stopped_at is null
			`,
		)
		if err != nil {
			return nil, err
		}
		setJobWorkerAliveExec = execFunc
		setJobWorkerAliveClose = closeStmt
	}
	return setJobWorkerAliveExec, nil
}

// closeCachedStmts closes the package-level cached prepared statements (the claim
// statement and the heartbeat statement) and resets them to nil so a later
// InitJobQueue prepares them again. It is called from jobworkerDB.Close, after
// the closed flag is set, so no new claim or heartbeat starts using them.
func closeCachedStmts() error {
	claimJobStmtMtx.Lock()
	var errClaim error
	if claimJobStmtClose != nil {
		errClaim = claimJobStmtClose()
		claimJobStmtQuery, claimJobStmtClose = nil, nil
	}
	claimJobStmtMtx.Unlock()

	setJobWorkerAliveMtx.Lock()
	var errAlive error
	if setJobWorkerAliveClose != nil {
		errAlive = setJobWorkerAliveClose()
		setJobWorkerAliveExec, setJobWorkerAliveClose = nil, nil
	}
	setJobWorkerAliveMtx.Unlock()

	return errors.Join(errClaim, errAlive)
}

func (j *jobworkerDB) SetJobWorkerAlive(ctx context.Context, jobID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	// The heartbeat fires repeatedly (every HeartbeatInterval per in-flight job),
	// so it runs as a cached prepared statement with the job id as its only
	// bind parameter.
	exec, err := setJobWorkerAliveStmt(ctx)
	if err != nil {
		return err
	}
	return exec(ctx, jobID) // $1 = jobID
}

func (j *jobworkerDB) ScheduleRetry(ctx context.Context, jobID uu.ID, startAt time.Time, retryCount int) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID, startAt)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `
			update worker.job
			set
				start_at=$1,
				started_at=null,
				stopped_at=null,
				error_msg=null,
				error_data=null,
				worker_alive_at=null,
				current_retry_count=$2,
				updated_at=now()
			where id = $3
		`,
		startAt,    // $1
		retryCount, // $2
		jobID,      // $3
	)
}

func (j *jobworkerDB) DeleteJob(ctx context.Context, jobID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobID)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `delete from worker.job where id = $1`, jobID,
	)
}

func (j *jobworkerDB) DeleteJobsFromOrigin(ctx context.Context, origin string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, origin)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `delete from worker.job where origin = $1`, origin,
	)
}

func (j *jobworkerDB) DeleteJobsOfType(ctx context.Context, jobType string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobType)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `delete from worker.job where type = $1`, jobType,
	)
}

func (j *jobworkerDB) DeleteFinishedJobs(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `
			delete from worker.job
			where stopped_at is not null
				and	error_msg is null
				and	bundle_id is null
		`,
	)
}

func (j *jobworkerDB) GetJobBundle(ctx context.Context, jobBundleID uu.ID) (jobBundle *jobqueue.JobBundle, err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID)

	if j.closed.Load() {
		return nil, jobqueue.ErrClosed
	}

	err = db.TransactionReadOnly(ctx, func(ctx context.Context) error {
		jobBundle, err = db.QueryRowAs[*jobqueue.JobBundle](ctx,
			/*sql*/ `
				select *
				from worker.job_bundle
				where id = $1
			`,
			jobBundleID, // $1
		)
		if err != nil {
			return err
		}

		jobBundle.Jobs, err = db.QueryRowsAsSlice[*jobqueue.Job](ctx,
			/*sql*/ `
				select *
				from worker.job
				where bundle_id = $1
				order by created_at
			`,
			jobBundleID, // $1
		)
		return err
	})
	if err != nil {
		return nil, err
	}

	return jobBundle, nil
}

func (j *jobworkerDB) DeleteJobBundle(ctx context.Context, jobBundleID uu.ID) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, jobBundleID)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `delete from worker.job_bundle where id = $1`, jobBundleID,
	)
}

func (j *jobworkerDB) DeleteJobBundlesFromOrigin(ctx context.Context, origin string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, origin)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `delete from worker.job_bundle where origin = $1`, origin,
	)
}

func (j *jobworkerDB) DeleteJobBundlesOfType(ctx context.Context, bundleType string) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx, bundleType)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Exec(ctx,
		/*sql*/ `delete from worker.job_bundle where type = $1`, bundleType,
	)
}

func (j *jobworkerDB) DeleteAllJobsAndBundles(ctx context.Context) (err error) {
	defer errs.WrapWithFuncParams(&err, ctx)

	if j.closed.Load() {
		return jobqueue.ErrClosed
	}

	return db.Transaction(ctx, func(ctx context.Context) error {
		err = db.Exec(ctx,
			/*sql*/ `delete from worker.job_bundle`,
		)
		if err != nil {
			return err
		}
		return db.Exec(ctx,
			/*sql*/ `delete from worker.job`,
		)
	})
}
