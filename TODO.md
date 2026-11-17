# TODO

## Job-query methods for `jobqueue.Service`

Applications that need queries this package doesn't offer tend to reach into the
`worker.job` / `worker.job_bundle` tables with raw SQL. Each item below adds a
`jobqueue.Service` method so callers can drop such SQL. Every new `Service`
method ripples to four places: the interface and a package-level delegating func
in `service.go`, the real implementation in `jobworkerdb/jobworkerdb.go`, and the
stubs in `donothingservice.go` and `errors.go` (plus a test under `tests/`).

### Done

- [x] **`DeleteFinishedJobs(ctx, finishedFor time.Duration)`** — age-bounded delete
  of finished, error-free, non-bundled jobs. `finishedFor == 0` keeps the old
  unconditional behaviour; `> 0` deletes only jobs whose `stopped_at` is older
  than the cutoff, evaluated with the DB clock (`now() - make_interval`) so it is
  skew-immune; `< 0` returns an error.

- [x] **`GetJobsWithTypeAndPayload` / `HasJobWithTypeAndPayload`** — jobs of a type
  whose payload equals the passed payload (compared as `jsonb`), filtered by
  stopped state; `Has` is the existence-only companion (`SELECT EXISTS`).
  Whole-payload equality matches a single-key lookup only while the payload type
  has exactly that one field. If a caller needs to match a *subset* of payload
  keys, add a `jsonb` containment variant (`payload @> $`).

### Pending

- [ ] **`GetJobs(ctx, JobQuery) ([]*Job, error)`** — filtered/searched job list
  (status + ILIKE search over id/type/origin/error_msg/payload + limit, newest
  first) for job dashboards. Introduce a `JobStatus` vocabulary (`pending`,
  `running`, `long_running`, `dead`, `errors`, `completed`) and a `JobQuery`
  struct carrying the `LongRunningFor` / `DeadFor` thresholds. Classify
  running/long_running/dead DB-side against the DB clock (skew-immune).
  Open question: `DeadFor` is already a package concept (`Job.WorkerAlive`,
  the reaper), but `LongRunningFor` is presentation policy — decide whether it
  belongs in the package or stays a caller concern.

- [ ] **`GetJobStatusCounts(ctx, longRunningFor, deadFor time.Duration) (*JobStatusCounts, error)`**
  — one-pass `count(*) FILTER (...)` aggregate returning pending/running/errors/
  long_running/dead counts. Keep separate from `GetStatus` (which has no
  thresholds).

- [ ] **Debounce / "ModifyJob"** — atomically amend the oldest not-yet-started job
  of a type, or insert a new one, under a non-blocking row lock
  (`FOR UPDATE SKIP LOCKED`, deadlock-safe inside an enclosing transaction and
  across processes). Sketch:
  `AddOrAmendPendingJob(ctx, newJob *Job, amend func(existing, incoming *Job) (notnull.JSON, error)) error`.
  Largest design of the set — give it its own PR with tests for the lock
  semantics and the SKIP-LOCKED race (worst case: two pending jobs instead of one
  merge).
