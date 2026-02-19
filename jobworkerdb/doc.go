/*
Package jobworkerdb provides the PostgreSQL implementation of the jobqueue service.

# Overview

The jobworkerdb package implements the jobqueue.Service and jobworker.DataBase
interfaces using PostgreSQL as the backend. It handles job persistence,
retrieval, and uses PostgreSQL's LISTEN/NOTIFY for real-time job notifications.

# Initialization

Initialize the job queue with [InitJobQueue] which creates the service,
registers it as the default for both jobqueue and jobworker packages,
and resets any jobs interrupted by a previous shutdown or crash:

	err := jobworkerdb.InitJobQueue(ctx)

The database connection must be set up before calling InitJobQueue:

	import "github.com/domonda/go-sqldb/db"
	db.SetConn(postgresConnection)

# Database Schema

The package requires the worker schema in PostgreSQL with:
  - worker.job table
  - worker.job_bundle table
  - PostgreSQL triggers for LISTEN/NOTIFY notifications

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
