/*
Package jobworkerdb provides the PostgreSQL implementation of the jobqueue service.

# Overview

The jobworkerdb package implements the jobqueue.Service and jobworker.DataBase
interfaces using PostgreSQL as the backend. It handles job persistence,
retrieval, and uses PostgreSQL's LISTEN/NOTIFY for real-time job notifications.

# Initialization

Initialize the job queue with [InitJobQueue] which creates the service and
registers it as the default for both the jobqueue and jobworker packages.
InitJobQueue does not reset any jobs:

	err := jobworkerdb.InitJobQueue(ctx)

The database connection must be set up before calling InitJobQueue:

	import "github.com/domonda/go-sqldb/db"
	db.SetConn(postgresConnection)

To additionally reclaim jobs that were abandoned by a worker that crashed at
least deadFor ago, use [InitJobQueueResetInterruptedJobs]. It only resets jobs
whose worker is provably dead, so — as long as deadFor is at least
3 × jobworker.HeartbeatInterval — it is safe to run on startup even when multiple
worker processes share the same database:

	err := jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, deadFor)

# Database Schema

The package requires the worker schema in PostgreSQL with:
  - worker.job table
  - worker.job_bundle table
  - PostgreSQL triggers for LISTEN/NOTIFY notifications

The schema/ files describe a fresh install. When upgrading an existing database
to the heartbeat-based reaper, apply these out-of-band before running this code.

Add the worker.job.worker_alive_at column (a "select *" into jobqueue.Job
otherwise fails on the missing column):

	alter table worker.job add column if not exists worker_alive_at timestamptz;

The fresh-install schema also ships a partial index on the bundle_id foreign key
(it backs the job_bundle ON DELETE CASCADE, which Postgres does not auto-index).
An upgraded database is missing it, so add it too — concurrently, since this runs
against a live database outside a transaction:

	create index concurrently if not exists worker_job_bundle_id_idx
		on worker.job(bundle_id) where bundle_id is not null;

Jobs that were mid-execution at the moment of that upgrade have a NULL
worker_alive_at, which the in-progress branch of [InitJobQueueResetInterruptedJobs]
skips (it only resets jobs with a stale, non-NULL heartbeat). Backfill their
worker_alive_at to started_at once, as part of the upgrade, so they look like a
job started but with a stale heartbeat — started_at is necessarily older than the
restart, so the value is already stale. The reaper then reclaims them like any
other abandoned job, subject to its deadFor grace period:

	update worker.job
	set worker_alive_at=started_at, updated_at=now()
	where started_at is not null and stopped_at is null and worker_alive_at is null;

# LISTEN/NOTIFY

The service uses PostgreSQL LISTEN/NOTIFY for real-time job notifications:
  - job_available: Fired when a new job is ready to process
  - job_stopped: Fired when a job completes
  - job_bundle_stopped: Fired when all jobs in a bundle complete

# Testing Utilities

The package provides context utilities for testing:

Synchronous job execution (no database persistence):

	ctx = jobworkerdb.ContextWithSynchronousJobs(ctx)
	jobqueue.Add(ctx, job) // Executes immediately

Ignore all jobs using the [IgnoreAllJobs] predicate:

	ctx = jobworkerdb.ContextWithIgnoreJob(ctx, jobworkerdb.IgnoreAllJobs)
	jobqueue.Add(ctx, job) // Job is discarded

Ignore jobs of a specific type:

	ctx = jobworkerdb.ContextWithIgnoreJobType(ctx, "my-job-type")

Ignore all job bundles using the [IgnoreAllJobBundles] predicate:

	ctx = jobworkerdb.ContextWithIgnoreJobBundle(ctx, jobworkerdb.IgnoreAllJobBundles)
	jobqueue.AddBundle(ctx, bundle) // Bundle is discarded

Custom filtering with [IgnoreJobFunc] and [IgnoreJobBundleFunc]:

	ctx = jobworkerdb.ContextWithIgnoreJob(ctx, func(job *jobqueue.Job) bool {
		return job.Type == "skip-this"
	})

# Transactions

The implementation uses database transactions to ensure consistency:
  - Job bundles are inserted atomically with all their jobs
  - Job completion updates job_bundle.num_jobs_stopped in a transaction
*/
package jobworkerdb
