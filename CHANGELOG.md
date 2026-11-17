# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0] - 2026-11-17

You can now ask the queue whether a job with a given type and payload exists,
or fetch those jobs, and delete only jobs that finished at least a given time
ago. Code that queried `worker.job` with its own SQL for these can switch to the
`jobqueue` API. `DeleteFinishedJobs` takes a new argument and `jobqueue.Service`
gains two methods, so callers and `Service` implementations need small changes.

### Added

- `jobqueue.GetJobsWithTypeAndPayload(ctx, jobType, payload, stopped)` returns
  the jobs of `jobType` whose payload equals `payload`, and
  `jobqueue.HasJobWithTypeAndPayload` reports whether one exists without loading
  it. Payloads are compared as PostgreSQL `jsonb`, so key order and whitespace
  don't matter. `stopped` selects stopped jobs (finished, failed or paused for a
  decision) or jobs still to be processed. Use `Has…(ctx, typ, payload, false)`
  to check whether a job is already queued or running; the check and a following
  `Add` are not atomic.

- A lookup payload that can't be a job payload (nil, empty, invalid JSON or
  `null`), or that contains characters PostgreSQL can't store such as a `\u0000`
  escape, returns an error matching `jobqueue.ErrInvalidPayload`. The payload is
  compared exactly as given, so a job whose payload had such characters removed
  on write is found by its stored form. Lookup errors don't quote the payload,
  apart from what a marshalling error names (see `MarshalJobPayload`).

- **Schema:** the new index `worker_job_unstopped_payload_idx` (in
  `schema/worker/job.sql`) makes the payload lookups for jobs still to be
  processed an index probe instead of a scan over every unstopped job. Apply it
  to existing databases with
  `create index worker_job_unstopped_payload_idx on worker.job using hash (payload) where stopped_at is null;`
  (add `concurrently` to avoid blocking writes on a large table).

- `jobqueue.MarshalJobPayload(payload)` exposes the conversion `NewJob` applies
  to a payload, for code that needs the JSON a job would store.

### Changed

- **Breaking:** `DeleteFinishedJobs(ctx)` is now
  `DeleteFinishedJobs(ctx, finishedFor time.Duration)`. Pass `0` for the old
  behavior of deleting every finished job without errors that isn't part of a
  bundle. A positive `finishedFor` deletes only jobs that finished at least that
  long ago by the database clock, so clock differences between servers don't
  matter. A negative `finishedFor` returns an error and deletes nothing.

- Errors no longer quote job payloads, which can carry personal data and get
  logged. An error that wraps a `Job` or a `JobBundle`, such as those of
  `jobqueue.Add`, `jobqueue.AddBundle` and a failed worker, shows the payload as
  `***REDACTED*** (N bytes)` while keeping the job's ID, type and other fields,
  and a database error from inserting a job leaves the payload out of the query
  it quotes. `Job` implements go-pretty's `Printable` for this.

- **Breaking:** the `jobqueue.Service` interface has the two new methods
  `GetJobsWithTypeAndPayload` and `HasJobWithTypeAndPayload`. A custom `Service`
  implementation or mock must add them.

## [0.8.1] - 2026-11-17

The domonda dependencies move to their latest releases. The job queue's own
code is unchanged, but importing v0.8.1 raises `go-errs` in your module from
v1.0.4 to v1.3.1, which changes some error behavior you may rely on.

### Changed

- `github.com/domonda/go-errs` v1.3.1 (from v1.0.4). Errors created with
  `errs.New`, `errs.Errorf`, `errs.WrapWithCallStack` and
  `errs.WrapWithFuncParams` render their message once, on the first `Error()`
  call, and return that text afterward. So a job error wrapped by the worker
  shows the job as it was when the error was first printed. Two such errors
  that are otherwise equal no longer compare equal with `reflect.DeepEqual` or
  testify's `assert.Equal` when only one has been printed; compare `Error()`
  strings or use `errors.Is`. `errs.AsErrorWithDebugStack` and
  `errs.RecoverPanicAsError` return unexported error types; their messages and
  `Unwrap` results are unchanged.

- The `go-errs-wrap` tool now lives in its own module,
  `github.com/domonda/go-errs/cmd/go-errs-wrap`. If your `go.mod` has a
  `tool github.com/domonda/go-errs/cmd/go-errs-wrap` line from before go-errs
  v1.1.0, `go tool go-errs-wrap` stops working after this upgrade. To restore
  it, run `go get github.com/domonda/go-errs@latest`, then
  `go get -tool github.com/domonda/go-errs/cmd/go-errs-wrap@latest`, then
  `go mod tidy`.

- `github.com/domonda/go-sqldb` and `github.com/domonda/go-sqldb/pqconn`
  v1.4.1, `github.com/domonda/golog` v1.3.1, and the indirect
  `DataDog/go-sqllexer`, `go.yaml.in/yaml/v3`, `golang.org/x/sys` and
  `golang.org/x/term` dependencies move to their latest patch releases.

## [0.8.0] - 2026-10-18

`go-types` moves to its tagged `v1.0.0-beta.2`, `golog` to v1.3.0, and building
this module now requires Go 1.27.0. Job payloads are validated more strictly and
in one place: `NewJob` and `jobqueue.Add`/`AddBundle` with the PostgreSQL service
now reject an empty, invalid or JSON `null` payload and a nil pointer payload,
including for hand-built `Job` values, with an error matching the new
`jobqueue.ErrInvalidPayload`.

### Added

- `jobqueue.ErrInvalidPayload`, matched by every payload rejection; test for it
  with `errors.Is`. A rejected payload is rejected again, so don't retry it
  unchanged. `jobqueue.InvalidPayloadError(reason, cause)` creates such an error
  for `Service` implementations. It doesn't wrap the sentinel or the cause, so
  `errs.Root` and log titles keep the reason instead of the bare sentinel.

- `jobqueue.ValidatePayload(payload []byte) error`, the payload rules `NewJob`
  and the queue apply: it rejects an empty payload ("invalid job payload:
  empty"), invalid JSON ("invalid job payload: not valid JSON (N bytes)") and the
  JSON `null` value ("invalid job payload: JSON null").

### Changed

- **Breaking:** `NewJob`, `NewJobWithPriority`, `NewJobReflectType` and
  `NewJobBundle` reject a payload that is JSON `null`, with or without
  surrounding whitespace. That includes `"null"`, `nullable.JSON("null")`, and
  nil maps and slices or a `MarshalJSON` method that marshal to `null` ("invalid
  job payload: T marshals to JSON null"). A `null` payload carries no data, so
  its worker would run on a zero value. `null` nested inside a payload object is
  still accepted.

- **Breaking:** a nil pointer payload of any type is rejected ("invalid job
  payload: nil *T") instead of being marshalled, usually to `null`. An untyped
  nil payload reports "invalid job payload: nil".

- **Breaking:** a payload with its own `MarshalJSON` method is marshalled with
  `encoding/json`, which validates the method's output and compacts it, escaping
  `<`, `>` and `&` like in any other marshalled payload. Empty or invalid output
  is rejected ("invalid job payload: T can't be marshalled to JSON") instead of
  being queued. The JSON value of valid output is unchanged.

- **Breaking:** a pointer to `notnull.JSON`, `nullable.JSON` or
  `json.RawMessage` is dereferenced and its JSON text validated like the value,
  instead of being marshalled. A pointer to an empty `notnull.JSON` used to be
  queued as `{}` and is now rejected as an empty payload.

- **Breaking:** `jobqueue.Add` and `jobqueue.AddBundle` with the PostgreSQL
  service check every job's payload with `ValidatePayload`, so a hand-built `Job`
  with an empty, invalid or `null` payload fails instead of being stored as `{}`
  or jsonb `null`. The check applies to the payload the caller passed, with a raw
  zero byte counted as a space: a zero byte inside a string is still removed by
  sanitizing and the job stored, but bytes that break the JSON structure, like
  invalid UTF-8 in a number, are rejected instead of being sanitized into a
  different value. The check runs before a job is ignored
  (`ContextWithIgnoreJob`, `ContextWithIgnoreJobType`,
  `ContextWithIgnoreJobBundle`), run in a `ContextWithSynchronousJobs` context or
  stored, so all three modes accept the same payloads and tests that stub
  enqueueing see the same rejections. A synchronous run gets the sanitized
  payload that would be stored (jsonb may still normalize numbers and key order
  on read), and the caller's payload is restored afterwards. A bundle is checked as a whole before its first
  row is inserted: a job with a rejected payload fails the bundle, also inside a
  caller's outer transaction. A payload with nothing storable left reports
  "invalid job payload: no characters PostgreSQL can store". Not covered by the
  check: a number beyond the range of PostgreSQL's numeric type fails on insert
  with a database error, and sanitizing can make object keys that differ only in
  removed characters equal (jsonb keeps the last of duplicate keys).

- `NewJob` payload errors no longer quote the payload, which can be large or
  carry personal data. The error of a payload that can't be marshalled keeps
  `encoding/json`'s message, which can name a field or quote a single value
  such as an invalid `json.Number`, and includes verbatim the error of any
  `MarshalJSON` or `MarshalText` method it called, on the payload or a value
  inside it. The errors of `jobqueue.Add` and `AddBundle` still list
  the job or bundle in their call context, payload included, as before.

- `NewJobBundle` errors name the rejected description, as in
  `jobDescriptions[2] of type "my-job": invalid job payload: JSON null`.

- `NewJob` now returns a nil job with its error for payloads it used to accept
  (nil maps, slices and pointers, `null`). Code that discards the error, as in
  `job, _ := jobqueue.NewJob(...)`, now gets "nil job" from `jobqueue.Add`
  instead of a queued job: check the error of `NewJob`.

- The rules apply to jobs added by this release. Jobs queued by an earlier
  release or written by other producers can still carry a `null` payload. To
  find unfinished ones before upgrading workers:
  `select id, type from worker.job where jsonb_typeof(payload) = 'null' and stopped_at is null`.

- Go toolchain **1.26.0 → 1.27.0** (the `go` directive in `go.mod` and
  `tools/go.mod`), so building this module now requires Go 1.27.0 or newer. CI
  tracks the latest 1.27.x patch release.

- `go-types` `v0.0.0-20260902160716-5c6c50f32fcd` → **`v1.0.0-beta.2`**. Go's
  minimal version selection uses the highest version any module requires, and
  `v1.0.0-beta.2` sorts above every `v0.0.0` pseudo-version, so importing this
  release upgrades `go-types` in your build to at least `v1.0.0-beta.2`. Its
  [CHANGELOG](https://github.com/domonda/go-types/blob/v1.0.0-beta.2/CHANGELOG.md)
  lists the breaking changes. The ones closest to job queue code:
  - `uu.NewID`, which creates job and bundle IDs, makes version 7 UUIDs, and
    their timestamp is now stored big-endian as RFC 9562 requires. `ID.V7Time`
    now returns `(time.Time, bool)` and misreads IDs generated by earlier
    `go-types` versions, and `ID.Less` and `IDSlice.Sort` order IDs differently.
    go-jobqueue itself never decodes or orders by job ID.
  - The ID function of `uu.ContextWithIDSequence` panics when its sequence runs
    out instead of returning the nil UUID. `NewJobBundle` takes one ID per job
    plus one for the bundle.
  - An empty `nullable.JSON` is now null (`IsNull`, `Value`), and an empty
    `notnull.JSON` now counts as `{}` (`Valid`, `Value`, `UnmarshalTo`). A
    `RegisterFunc` worker called directly through `jobworker.DoJob` with a
    hand-built job whose payload is an empty, non-nil `notnull.JSON` now
    receives the zero value instead of failing. Jobs added through the queue or
    read from the database never carry an empty payload.
  - `RegisterFunc` and `RegisterFuncForJobType` now panic at registration for a
    map argument or result type whose key implements `encoding.TextMarshaler`
    only through its pointer type, because `types.CanMarshalJSON` rejects it
    (`encoding/json` cannot marshal such keys either).

- `golog` v1.2.1 → v1.3.0. The root `golog` package this module uses is
  unchanged; the release raises its own minimum Go version and moves `logsentry`
  to `sentry-go` v0.50.0.

- Indirect dependencies followed. `pb33f/ordered-map/v2` v2.3.2 replaces
  `go.yaml.in/yaml/v4` `v4.0.0-rc.4` with `pb33f/go-yaml` v0.1.1, so the
  release-candidate `yaml/v4` pin described under 0.7.3 is gone.

### Fixed

- `NewJob` and its variants no longer panic for a nil `*notnull.JSON` payload,
  whose value-receiver `MarshalJSON` was called through the nil pointer.

- `jobqueue.Add` and `jobqueue.AddBundle` with the PostgreSQL service return an
  error instead of panicking for a nil job, a nil bundle or a nil job in a
  bundle. A bundle rejected before anything is stored (nil job, invalid
  payload, closed service) leaves its jobs unmodified; once those checks pass,
  its jobs carry the bundle's ID even if a synchronous run or the database
  then fails, as before.

- `NewJobReflectType` and `NewJobBundle` (for a `JobDesc` without a `Type`) no
  longer panic for a nil payload while deriving the job type, and return the
  "invalid job payload: nil" error. `ReflectJobTypeOfPayload(nil)` and
  `JobTypeOfPayloadType(nil)` return an empty string.

- `NewJob`, `NewJobWithPriority`, `NewJobReflectType` and `NewJobBundle` keep
  rejecting an empty or nil payload (`""`, `[]byte{}`, `json.RawMessage(nil)`,
  an empty or nil `notnull.JSON`, `nullable.JSON(nil)`), now with "invalid job
  payload: empty". `go-types` v1.0.0-beta.2 made `notnull.JSON.Valid` report
  empty input as valid, which would have queued such a job as `{}` and run its
  worker on a zero-value payload.

## [0.7.3] - 2026-09-03

Dependency-only release: every dependency moved to its latest stable version. No
source file changed and no public API moved.

### Changed

- Direct dependencies updated: `go-errs` v1.0.3 → v1.0.4, `go-sqldb` and
  `go-sqldb/pqconn` v1.3.0 → v1.4.0, `golog` v1.1.1 → v1.2.1, `go-types` to
  `v0.0.0-20260902160716-5c6c50f32fcd`, and `testify` v1.11.1 → v1.12.1.

- Job errors reported through Sentry get better. `go-errs` v1.0.4 gives wrapped
  errors a `StackTrace() []uintptr` method in the `pkg/errors` shape, and `golog`
  v1.2.0 makes `logsentry` surface a logged `Err` as a Sentry exception carrying
  that stack trace, with the error text as the issue subtitle instead of
  "(No error message)". A consumer running the Sentry log writer sees job failure
  issues change shape without changing anything on their side.

- `go-sqldb` v1.4.0 reworked `ScanDriverValue` into a best-effort converter that
  mirrors the `database/sql` conversions, so scanning is more permissive than
  before, not less.

- Lint and security tooling in `tools/go.mod` updated: `revive` v1.15.0 → v1.16.0
  and `gosec` v2.27.1 → v2.29.0. Neither reports a new finding on this codebase.

  Two indirect modules cannot be pinned to a stable version because upstream has
  published none: `go-types` has no semver tags at all, so it stays a
  pseudo-version, and `go.yaml.in/yaml/v4` (pulled in through `go-types` and
  `testify`) has only release candidates. `yaml/v4` is held at `v4.0.0-rc.4`, the
  lowest release our own dependencies require, rather than the newest rc.

## [0.7.2] - 2026-07-30

Jobs carrying user provided data that PostgreSQL cannot store are now persisted
with the unstorable characters removed, instead of failing the write and losing
the job.

### Changed

- **Writes that used to fail now succeed with characters removed.** `jobworkerdb`
  strips what PostgreSQL cannot store from the job origin, payload, error message,
  error data and result, and from the job bundle origin: zero bytes, invalid UTF-8,
  and two escape sequences that jsonb rejects even though they are valid JSON —
  the zero byte escape (what `encoding/json` emits for a zero byte inside a Go
  string) and unpaired surrogates (what a browser's `JSON.stringify` emits for a
  truncated emoji). Because both are accepted by `json.Valid`, nothing upstream
  refused such a payload before and only the insert failed.

  This is a behaviour change from fail-closed to silently transformed: a value read
  back from the database may be missing characters that were present in the `Job`
  passed to `Add`. The in-memory `Job` is never modified. Sanitizing deletes rather
  than replaces, so it joins whatever surrounded a removed character — validate
  values *after* sanitizing, not before, and see the `jobworkerdb` package docs for
  the full list of consequences, including that sanitizing is not injective.

- Sanitizing applies only to values being written, never to a value used to look
  rows up. `DeleteJobsFromOrigin` and `DeleteJobBundlesFromOrigin` match their
  argument verbatim, so a job whose origin had to be sanitized on write is
  reachable by the sanitized string or by ID, but not by the original. Normalizing
  a destructive unbounded key would be worse: sanitizing is not injective, so it
  could collapse one origin onto another and delete jobs the caller never named.

- A payload with nothing storable left now fails the write instead of being stored
  as an empty object. `notnull.JSON` binds a nil value as `{}`, so such a job would
  otherwise have been queued and dispatched to a worker as an all-zero payload.

- `Job.Type` and `JobBundle.Type` are deliberately **not** sanitized, so an
  unstorable type still fails the write loudly: `Job.Type` is the dispatch key a
  worker registers for, and storing a rewritten one would queue a job no worker
  could ever claim.

- An error message that ends up empty is replaced by a placeholder rather than
  stored as `''`, which would read back as SQL NULL through
  `nullable.NonEmptyString` and make a failed job report `Succeeded() == true`.
  The two ways it can be empty get distinct texts, so the row does not claim
  characters were removed when the caller simply had nothing to say.

- Removing characters can join a backslash to the text behind it and form one of
  the rejected escapes that was not in the input, so stripping repeats — a bounded
  number of times rather than until stable. Crafted malformed JSON can chain those
  seams so that each round exposes exactly one more, which would make the work
  quadratic on the write path. A value still unstorable after the cap is malformed
  JSON that PostgreSQL rejects on its own, so the write fails loudly instead.

### Added

- A `WARN` log line whenever sanitizing actually removes characters, naming the
  job or bundle id, the field, and the number of bytes dropped, so that a stored
  value differing from what a producer sent is traceable instead of invisible.

### Performance

- Sanitizing runs on every job write and payloads can be large, so the
  already-storable case — which is essentially every real payload — scans in place
  and allocates nothing: 0 allocations for a 1MB payload, one allocation when
  something is actually removed.

## [0.7.1] - 2026-07-02

Quieter shutdowns: a job dispatch cancelled during shutdown is no longer reported
as an error.

### Changed

- A cancelled job (`context.Canceled`) is now treated as an expected interruption
  rather than a failure. When a worker function returns a `context.Canceled` error
  (directly or wrapped), or a job dispatch returns one because the worker-thread
  context was cancelled (e.g. during shutdown), `jobworker` no longer invokes the
  `OnError` callback or logs the "Job error" / "Error while dispatching the job"
  lines at `ERROR`/`WARN`. The job is still reset and retried without consuming a
  retry attempt, exactly as before — only the spurious error reporting is dropped.
  Genuine failures and per-job `JobTimeout` (`context.DeadlineExceeded`) are
  unaffected and still reported.

## [0.7.0] - 2026-06-18

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

## [0.6.0] - 2026-06-17

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

## [0.5.4] - 2026-03-31

Last release before the worker liveness heartbeat work. See the git history for
details of `v0.5.4` and earlier releases.

[0.8.0]: https://github.com/domonda/go-jobqueue/compare/v0.7.3...v0.8.0
[0.7.3]: https://github.com/domonda/go-jobqueue/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/domonda/go-jobqueue/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/domonda/go-jobqueue/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/domonda/go-jobqueue/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/domonda/go-jobqueue/compare/v0.5.4...v0.6.0
[0.5.4]: https://github.com/domonda/go-jobqueue/releases/tag/v0.5.4
