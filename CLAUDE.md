# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

PostgreSQL-backed job queue for Go with support for job bundles, retries, worker pools, and real-time notifications via LISTEN/NOTIFY.

## Common Commands

- **Run tests**: `./scripts/run-tests.sh` (creates temp DB, applies schema, runs tests, drops DB)
- **Run specific tests**: `./scripts/run-tests.sh -- -run TestReset`
- **Verbose tests**: `./scripts/run-tests.sh -v`
- **Build check**: `go build -v ./...`

Tests require PostgreSQL. The script auto-starts one via `docker compose` if none is reachable at `POSTGRES_HOST:POSTGRES_PORT` (defaults from `.env.example`: `127.0.0.1:5432`, user `postgres`).

## Architecture

Three packages with clear layering:

- **`jobqueue`** (root): Core types (`Job`, `JobBundle`, `Status`), `Service` interface, and package-level functions that delegate to a context-or-default service. No database dependency.
- **`jobworker`**: Worker registration (`Register`, `RegisterFunc`, `RegisterFuncForJobType`), job execution logic, thread pool management (`StartThreads`, `FinishThreads`), and retry scheduling.
- **`jobworkerdb`**: PostgreSQL implementation of both `jobqueue.Service` and `jobworker.DataBase`. All SQL lives here. Handles LISTEN/NOTIFY for `job_available`, `job_stopped`, and `job_bundle_stopped` channels.

### Service wiring

`jobworkerdb.InitJobQueue(ctx)` (or `InitJobQueueResetDanglingJobs`) creates the DB-backed service, registers it as default for both `jobqueue` and `jobworker`, sets up LISTEN/NOTIFY listeners, and resets interrupted retryable jobs.

### Database schema

Schema files in `schema/`: `worker.sql` orchestrates `worker/job.sql`, `worker/job_bundle.sql`, `worker/job_triggers.sql`. Uses `worker` schema with tables `worker.job` and `worker.job_bundle`. Triggers fire PostgreSQL NOTIFY on job availability and completion.

### Testing helpers

- `jobworkerdb.ContextWithSynchronousJobs(ctx)` — executes jobs inline without DB persistence
- `jobworkerdb.ContextWithIgnoreJob(ctx, ...)` — silently discards jobs
- `jobworkerdb.ContextWithIgnoreJobType(ctx, jobType)` — discards specific job types
- `jobworkerdb.ContextWithIgnoreJobBundle(ctx, ...)` — discards bundles

### Job bundle counter invariant

Bundle completion tracking uses `num_jobs_stopped` counter. `SetJobResult`/`SetJobError` use `FOR UPDATE` (blocking, not `SKIP LOCKED`) on the bundle row because every completion must increment. `ResetJob` decrements the counter for already-counted jobs. This differs from `StartNextJobOrNil` which uses `FOR UPDATE SKIP LOCKED` since workers compete for any unclaimed job.

## Go Conventions

Follow the conventions from the parent project (domonda-service):

- Use `errs.New`/`errs.Errorf` from `github.com/domonda/go-errs` instead of `errors.New`/`fmt.Errorf`
- Every exported function returning an error: name the result `err`, add `defer errs.WrapWithFuncParams(&err, ...)` as first line, followed by an empty line
- Use `uu.ID` / `uu.IDSlice` from `github.com/domonda/go-types/uu` for UUIDs
- Use `github.com/domonda/go-sqldb/db` for SQL operations
- Prefix SQL string literals with `/*sql*/` and use backticks
- Use `github.com/stretchr/testify/require` and `assert` for tests
- Use `t.Context()` instead of `context.Background()` in tests
