/*
Package jobworkerdb provides the PostgreSQL implementation of the jobqueue service.

# Overview

The jobworkerdb package implements the jobqueue.Service interface using PostgreSQL
as the backend. It handles job persistence, retrieval, and uses PostgreSQL's
LISTEN/NOTIFY for real-time job notifications.

# Initialization

Create a new service instance:

	service := jobworkerdb.New()
	jobqueue.SetDefaultService(service)
	defer service.Close()

# Database Schema

The package requires the worker schema to be created in PostgreSQL.
Run the SQL files in the schema/ directory to set up:
- worker.job table
- worker.job_bundle table
- PostgreSQL triggers for notifications

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

Ignore jobs entirely:

	ctx = jobworkerdb.ContextWithIgnoreJobs(ctx)
	jobqueue.Add(ctx, job) // Job is discarded

Ignore job bundles:

	ctx = jobworkerdb.ContextWithIgnoreJobBundles(ctx)
	jobqueue.AddBundle(ctx, bundle) // Bundle is discarded

# Transactions

The implementation uses database transactions to ensure consistency:
- Job bundles are inserted atomically with all their jobs
- Job completion updates job_bundle.num_jobs_stopped in a transaction

# Connection Management

The package uses github.com/domonda/go-sqldb/db for database connections.
Set up the database connection before creating the service:

	import "github.com/domonda/go-sqldb/db"
	db.SetConn(postgresConnection)
*/
package jobworkerdb
