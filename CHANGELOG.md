# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.7.0] - 2026-06-18

Faster job claiming via a cached prepared statement, a dedicated claim index,
clock-skew-immune claim timestamps, and dependency updates.

### Added

- Partial index **`worker_job_claim_idx`** on
  `worker.job ("type", priority desc, created_at asc) where started_at is null`,
  backing the `StartNextJobOrNil` claim query — the hottest query in the queue.
  The partial predicate confines the index to the pending backlog (so it stays
  small as finished jobs accumulate), the leading `"type"` prunes to the
  registered types in the `in (...)` list, and the `priority desc, created_at asc`
  ordering matches the claim's `ORDER BY` so the scan yields rows in claim order:
  for a single registered type `limit 1` stops at the first unlocked row with no
  sort step, and for several types Postgres merges the per-type index streams
  instead of sorting the whole backlog.

### Changed

- **BREAKING (API):** `jobworker.RegisteredJobTypes()` now returns
  `(jobTypes []string, generation uint64)` instead of `notnull.StringArray`. The
  returned slice is a **cached, sorted, shared** value and **must not be mutated**
  by the caller. The `generation` counter is bumped on every `Register`/
  `Unregister` call (normally only at startup), so a consumer can cache a value
  derived from the types and rebuild only when the generation differs — no
  element-by-element comparison of the type sets.
- `jobworker.Register`, `RegisterFunc`, `RegisterFuncForJobType`, and `Unregister`
  now **panic if called while worker threads are running** (after `StartThreads`).
  Registration is startup-only: it mutates the registered-type set the cached claim
  statement is keyed on, so changing it mid-run would race that cache. Code that
  registers all workers before `StartThreads` is unaffected.
- `jobworkerdb` now claims and starts the next job in a **single CTE statement**
  (`with claimed as (select … for update skip locked) update worker.job … from
  claimed … returning worker.job.*`) with **no surrounding transaction**, run as a
  **cached prepared statement**. The registered job types are inlined as SQL string
  literals (`"type" in ('a','b')`, escaped via the connection's
  `FormatStringLiteral`) instead of an `= any($n)` array parameter, so PostgreSQL
  plans against the actual values. The statement is prepared once per process and
  re-prepared only when workers are registered or unregistered (normally
  startup-only). The per-heartbeat
  `SetJobWorkerAlive` update is likewise a cached prepared statement, and both are
  released on `Close`.
- Job-claim timestamps now use the **database clock** (`now()`): `started_at`,
  `updated_at`, `worker_alive_at`, and the `start_at <= now()` comparison are all
  evaluated server-side within the one claim statement, making job claiming immune
  to clock skew between worker processes and the database (consistent with the
  crash-recovery reaper, which already compares against `now()`). Previously
  `started_at` and `updated_at` were stamped from the worker process clock.

### Dependencies

- `github.com/domonda/go-sqldb` and `github.com/domonda/go-sqldb/pqconn`
  v1.0.2 → v1.3.0 (provides the prepared-statement helpers `QueryRowAsStmt` /
  `ExecStmt` and `FormatStringLiteral` this release builds on).
- `github.com/domonda/go-errs` v1.0.1 → v1.0.3, `github.com/domonda/golog`
  v1.0.5 → v1.1.1, and `github.com/domonda/go-types` updated, plus indirect
  dependency updates.
- Go toolchain **1.25.5 → 1.26.0** (the `go` directive in `go.mod` and
  `tools/go.mod`, and the CI `setup-go` version), so building this module now
  requires Go 1.26.0 or newer.

### For contributors

- The registered-job-types set is cached in `jobworker` (`workerTypes` +
  `workerTypesGeneration`, guarded by `workersMtx`) and invalidated on
  `Register`/`Unregister`, so the common claim path takes a single read lock with
  no allocation. `SetDataBase` and its backing `db` variable moved from
  `jobworker/database.go` to `jobworker/config.go` (no API change).
- `jobworkerdb` context keys switched from address-of-`int` sentinels to unexported
  empty-struct key types, and `reflect.Ptr` usages were updated to
  `reflect.Pointer`.
- Static analysis (`go vet`, `revive`, `gosec`) now runs before the database setup
  in `scripts/run-tests.sh` (skip with `-s`). `revive` and `gosec` are pinned in a
  separate `tools/go.mod` module (kept out of importers' dependency graph) and
  invoked via `go tool -modfile=tools/go.mod …`; `revive.toml` is report-only
  (`warningCode=0`/`errorCode=0`). The test run now covers `./...`, not just `tests/`.

### Migration

Upgrading an existing database from `v0.6.0` requires adding the new claim index.
No column or data changes are involved, so the code runs correctly without it —
only the `StartNextJobOrNil` claim query is slower until the index exists. Apply
it out-of-band:

```sql
-- Migration: v0.6.0 -> v0.7.0 (dedicated StartNextJobOrNil claim index)

-- Partial index backing the StartNextJobOrNil claim query (the hottest query in
-- the queue). The predicate confines it to the pending backlog, the leading
-- "type" prunes to the registered types, and priority desc, created_at asc
-- matches the claim's ORDER BY.
-- Use CREATE INDEX CONCURRENTLY (outside a transaction block) so building it on a
-- large, live table does not lock out job claims and inserts.
create index concurrently if not exists worker_job_claim_idx
    on worker.job ("type", priority desc, created_at asc)
    where started_at is null;
```

## [v0.6.0] - 2026-06-17

Worker liveness heartbeat and safe, multi-process crash recovery.

### Added

- **Worker liveness heartbeat.** While a worker processes a job it advances the
  new `worker.job.worker_alive_at` column every `jobworker.HeartbeatInterval`
  (default `10s`, set to `0` to disable) via the new
  `DataBase.SetJobWorkerAlive` method. A stale `worker_alive_at` while
  `stopped_at` is still NULL marks a job abandoned by a crashed worker.
- **`jobworkerdb.InitJobQueueResetInterruptedJobs(ctx, deadFor)`** — the
  multi-process-safe reaper. On startup it resets only jobs whose worker is
  *provably* dead (heartbeat stale by ≥ `deadFor`, or a pre-heartbeat worker
  left a job stuck between `SetJobError` and `ScheduleRetry` during a rolling
  upgrade, with a `stopped_at` older than `deadFor`).
  The cutoff is evaluated with the database clock, so it is immune to clock skew
  between worker processes and safe to run on every process's startup. Returns
  an error if `deadFor <= 0`, or — when heartbeats are enabled — if `deadFor` is
  not at least `3 × jobworker.HeartbeatInterval`.
- **`jobqueue.Job.WorkerAlive(deadFor)`** and **`jobqueue.Job.StartedAndNotStopped()`**
  predicate helpers. `WorkerAlive` compares the in-memory `WorkerAliveAt`
  snapshot against the local process clock; for an authoritative, skew-immune
  decision use the database-side reaper instead.
- **`jobqueue.Job.WorkerAliveAt`** field (`worker_alive_at timestamptz`).
- **`jobworker.HeartbeatInterval`** configuration variable.
- Partial index **`worker_job_bundle_id_idx`** on `worker.job(bundle_id)`
  (`where bundle_id is not null`), backing `GetJobBundle` lookups and the
  `ON DELETE CASCADE` from `worker.job_bundle`.
- README "Crash Recovery & Multiple Worker Processes" section and expanded
  package documentation.

### Changed

- **BREAKING (behavior):** `jobworkerdb.InitJobQueue` no longer resets
  interrupted jobs. It now only creates the service, registers it as the default
  for the `jobqueue` and `jobworker` packages, and sets up LISTEN/NOTIFY. To
  reclaim jobs abandoned by a crashed worker, call
  `InitJobQueueResetInterruptedJobs(ctx, deadFor)` instead.
- **BREAKING (API):** `jobworkerdb.InitJobQueueResetInterruptedJobs` now requires
  a `deadFor time.Duration` grace period; the previous signature took only `ctx`.
  Its old behavior (unconditionally reset every interrupted retryable job on
  startup) is replaced by resetting only provably-dead jobs older than `deadFor`.
- **BREAKING (API):** the `jobworker.DataBase` interface now requires the new
  `SetJobWorkerAlive(ctx, jobID)` method. Custom `DataBase` implementations must
  add it; `jobworkerdb` already provides it.
- **BREAKING (behavior):** `SetJobError` now records a *terminal* failure: it
  clamps `current_retry_count` up to `max_retry_count`. A job whose type has no
  registered retry scheduler (or whose scheduler errors) is therefore failed
  permanently and counted in its bundle, instead of being left
  stopped-with-retries-remaining — a state that previously stalled bundle
  completion and was reset and re-run by the reaper on every startup. To retry
  such a job after fixing the configuration, `ResetJob` it.
- `worker_alive_at` is intentionally left unindexed so the per-heartbeat rewrite
  keeps using Postgres HOT updates.
- Test database password retrieval now supports `PGPASSWORD`.

### Removed

- **BREAKING (API):** `jobworkerdb.InitJobQueueResetDanglingJobs` has been
  removed. Its single-instance behavior (reset every started-but-not-stopped job
  on startup) was unsafe with multiple worker processes, where another process
  may still be running the job. Use `InitJobQueueResetInterruptedJobs(ctx, deadFor)`,
  which reclaims only jobs whose worker is provably dead, instead.

  **Caveat for heartbeats-off setups:** the new reaper relies on the
  `worker_alive_at` heartbeat to detect a crashed worker. With
  `jobworker.HeartbeatInterval = 0` there is no liveness signal, so a job that
  was started but never stopped is **not** reclaimed (only jobs already errored
  with retries remaining are). If you ran a single worker process with heartbeats
  disabled and relied on `InitJobQueueResetDanglingJobs`, either keep heartbeats
  enabled or reset abandoned jobs yourself (e.g. `GetAllJobsStartedBefore` +
  `ResetJobs` once no worker is running).

### Migration

Upgrading an existing database from `v0.5.x` requires a schema change: the new
`worker_alive_at` column must exist before this code runs, because a
`select *` into `jobqueue.Job` fails on the missing column. Apply the following
out-of-band, before deploying `v0.6.0`:

```sql
begin;

-- Migration: v0.5.x -> v0.6.0 (worker liveness heartbeat + crash recovery)

-- 1. Add the worker_alive_at heartbeat column.
--    Required: jobqueue.Job selects this column ("select *"), so v0.6.0 code
--    fails against a database that lacks it.
alter table worker.job
    add column if not exists worker_alive_at timestamptz;

-- 2. Add the partial index on bundle_id backing GetJobBundle lookups and the
--    ON DELETE CASCADE from worker.job_bundle (Postgres does not auto-index
--    referencing columns). Partial because bundle_id is NULL for standalone jobs.
--    For a large, live table run this as CREATE INDEX CONCURRENTLY instead
--    (outside a transaction block) to avoid locking writes.
create index if not exists worker_job_bundle_id_idx
    on worker.job (bundle_id)
    where bundle_id is not null;

-- 3. Backfill worker_alive_at for jobs that were mid-execution at upgrade time.
--    They have a NULL worker_alive_at, which the startup reaper's in-progress
--    branch skips (it only resets jobs with a stale, non-NULL heartbeat).
--    Setting worker_alive_at = started_at marks them started-but-stale
--    (started_at predates the restart, so it is already stale), letting
--    InitJobQueueResetInterruptedJobs reclaim them like any other abandoned job,
--    subject to its deadFor grace period. Run this once as part of the upgrade.
update worker.job
set worker_alive_at = started_at,
    updated_at      = now()
where started_at is not null
  and stopped_at is null
  and worker_alive_at is null;

commit;
```

## [v0.5.4] - 2026-03-31

Last release before the worker liveness heartbeat work. See the git history for
details of `v0.5.4` and earlier releases.

[v0.7.0]: https://github.com/domonda/go-jobqueue/compare/v0.6.0...v0.7.0
[v0.6.0]: https://github.com/domonda/go-jobqueue/compare/v0.5.4...v0.6.0
[v0.5.4]: https://github.com/domonda/go-jobqueue/releases/tag/v0.5.4
