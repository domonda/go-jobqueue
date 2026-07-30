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

The fresh-install schema also ships a partial index backing the StartNextJobOrNil
claim query (the hottest query in the queue). An upgraded database is missing it,
so add it too — concurrently, against the live database outside a transaction.
Until it exists job claiming still works correctly, only slower:

	create index concurrently if not exists worker_job_claim_idx
		on worker.job("type", priority desc, created_at asc) where started_at is null;

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

# String Sanitization

Jobs routinely carry user provided data that PostgreSQL cannot store. It accepts
zero bytes in neither text nor jsonb columns and rejects invalid UTF-8. On top of
that, jsonb rejects two escape sequences that are perfectly valid JSON, which
matters because encoding/json and json.Valid both accept them, so nothing
upstream refuses such a payload and only the insert fails:

  - the escape for a zero byte, which is what encoding/json emits for a zero byte
    inside a Go string, so a marshalled payload carries no raw zero byte at all
  - unpaired surrogates, meaning a high surrogate not followed by a low one or a
    low surrogate not preceded by a high one. Complete surrogate pairs are valid
    and are kept. Browsers' JSON.stringify emits an unpaired surrogate for a
    truncated emoji.

Rather than failing the write and losing the job, this package silently removes
those characters as it writes. This applies to the job origin, payload, error
message, error data and result, and to the job bundle origin. Values read back
from the database therefore may be missing characters that were present in the
Job passed to AddJob — the in-memory Job itself is never modified.

Sanitizing applies to values being WRITTEN, never to a value used to look rows up.
[jobworkerDB.DeleteJobsFromOrigin] and [jobworkerDB.DeleteJobBundlesFromOrigin]
match their argument verbatim, so a job whose origin had to be sanitized on write
is reachable by the sanitized string or by ID, but not by the original. Normalizing
a destructive unbounded key would be worse: sanitizing is not injective, so it
could collapse one origin onto another and delete jobs the caller never named.

Three cases are not silently repaired, and fail the write instead. An origin
consisting entirely of unstorable characters sanitizes to empty and violates the
column's non-empty constraint. A payload with nothing storable left is rejected
rather than stored as an empty object, which would otherwise be dispatched to a
worker as an all-zero payload. And an error message that sanitizes to empty is
replaced by a placeholder rather than stored empty, because an empty error_msg
reads back as SQL NULL through nullable.NonEmptyString and would make a failed job
look successful.

Consequences worth knowing before relying on this:

Validating the value a producer sends does not constrain what a worker reads.
Sanitizing deletes characters rather than replacing them, so it joins whatever
surrounded a removed character: "a<zero byte>b" passes a producer-side check as two
tokens and is stored as "ab". A check that has to hold for the value a worker acts
on belongs in the worker, against what it read back, not in the producer. Before
this behaviour existed such a write failed instead, so this is a change from
fail-closed to silently transformed.

Sanitizing is not injective. Distinct inputs can collapse onto the same stored
value, so two origins differing only in unstorable characters become one origin in
the database, and an origin-keyed delete removes the jobs of both. Inside a
payload this can go further than losing characters: two JSON object keys that
differ only in unstorable characters become one duplicated key, and jsonb keeps
the last of them, so one field's value is replaced by another's rather than merely
shortened.

Raw control bytes other than the zero byte are not removed. PostgreSQL accepts them
in a text column, and in jsonb it tolerates only tab, newline and carriage return as
whitespace between tokens; any other raw control byte between tokens is rejected, as
is any raw control byte inside a jsonb string ("Character with value 0x01 must be
escaped"). Reaching either state requires JSON that was already malformed, since a
raw control byte inside a string is invalid JSON that json.Valid rejects, so such a
value is not repaired and the write still fails. Detecting them would mean another
full scan of every payload on the write path, on top of the two cheap vectorized
scans already done for zero bytes and UTF-8 validity.

Synchronous jobs are not sanitized. ContextWithSynchronousJobs executes jobs
without touching the database, so the worker receives the payload and origin
exactly as passed, while the same job persisted normally would be given the
sanitized values. Tests using that helper cannot reproduce what a production
worker reads.

The job and job bundle Type is deliberately not sanitized, so an unstorable Type
fails the write loudly instead. Job.Type is a dispatch key: jobworker looks a
worker up by it, so rewriting it would store a job no worker could ever claim,
which is worse than not storing it. JobBundle.Type is not a dispatch key — it is
only reported to bundle listeners — but it is kept verbatim for consistency with
Job.Type. jobqueue.NewJob checks Job.Type only for non-emptiness and NewJobBundle
does not check JobBundle.Type at all, so a caller that derives a Type from user data
has to keep it storable itself.

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
