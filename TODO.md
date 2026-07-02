# TODO

## Replace direct `worker.job` SQL in domonda-service with `jobqueue.Service` methods

`domonda-service` reaches into the `worker.job` / `worker.job_bundle` tables with
raw SQL in a handful of places instead of going through this package. Each item
below adds the `jobqueue.Service` method that lets the caller drop its direct SQL.
Every new `Service` method ripples to four places: the interface and a
package-level delegating func in `service.go`, the real implementation in
`jobworkerdb/jobworkerdb.go`, and the stubs in `donothingservice.go` and
`errors.go` (plus a test under `tests/`).

### Done

- [x] **`DeleteFinishedJobs(ctx, finishedFor time.Duration)`** — age-bounded delete
  of finished, error-free, non-bundled jobs. `finishedFor == 0` keeps the old
  unconditional behaviour; `> 0` deletes only jobs whose `stopped_at` is older
  than the cutoff, evaluated with the DB clock (`now() - make_interval`) so it is
  skew-immune.
  Replaces: `cmd/domonda-cron-scheduler/cron/config.go` ("Delete jobs finished an
  hour ago") → `jobqueue.DeleteFinishedJobs(ctx, time.Hour)`.

- [x] **`HasJobWithTypeAndPayload(ctx, jobType, payload, stopped) (bool, error)`** —
  existence-only companion to `GetJobsWithTypeAndPayload` (`SELECT EXISTS`, no rows
  fetched), payload compared as `jsonb`.
  Replaces: `pkg/apis/gmailsync/job.go` `HasUnfinishedImportEmailJob` →
  `jobqueue.HasJobWithTypeAndPayload(ctx, ImportEmailJobType, &ImportEmailJobPayload{GmailID: id}, false)`.
  Note: faithful for `ImportEmailJobPayload` because it has exactly the one
  `GmailID` field, so whole-payload equality == the old `payload->>'GmailID' = $`
  match. If a caller later needs to match a *subset* of payload keys, add a
  `jsonb` containment variant (`payload @> $`) rather than whole-payload equality.

### Pending

- [ ] **`GetJobs(ctx, JobQuery) ([]*Job, error)`** — filtered/searched job list
  (status + ILIKE search over id/type/origin/error_msg/payload + limit, newest
  first). Introduce a `JobStatus` vocabulary (`pending`, `running`, `long_running`,
  `dead`, `errors`, `completed`) and a `JobQuery` struct carrying the
  `LongRunningFor` / `DeadFor` thresholds. Classify running/long_running/dead
  DB-side against the DB clock (skew-immune), unlike the dashboard's current
  client-clock cutoffs.
  Replaces: `pkg/dashboard/jobqueue.go` `getFilteredJobs`.
  Open question: `DeadFor` is already a package concept (`Job.WorkerAlive`,
  the reaper), but `LongRunningFor` is presentation policy — decide whether it
  belongs in the package or stays a dashboard concern.

- [ ] **`GetJobStatusCounts(ctx, longRunningFor, deadFor time.Duration) (*JobStatusCounts, error)`**
  — one-pass `count(*) FILTER (...)` aggregate returning pending/running/errors/
  long_running/dead counts. Keep separate from `GetStatus` (which has no
  thresholds).
  Replaces: `pkg/dashboard/jobqueue.go` `getGlobalJobStats`.

- [ ] **Debounce / "ModifyJob"** — atomically amend the oldest not-yet-started job
  of a type, or insert a new one, under a non-blocking row lock
  (`FOR UPDATE SKIP LOCKED`, deadlock-safe inside an enclosing transaction and
  across processes). Sketch:
  `AddOrAmendPendingJob(ctx, newJob *Job, amend func(existing, incoming *Job) (notnull.JSON, error)) error`.
  Resolves the explicit `TODO add "ModifyJob" function to jobqueue package` in
  `pkg/banking/matching/job.go` (`EnqueueMatchInvoice`). Largest design of the
  set — give it its own PR with tests for the lock semantics and the SKIP-LOCKED
  race (worst case: two pending jobs instead of one merge).

### domonda-service follow-up

After the methods above ship and this package is tagged, migrate the call sites
to use them and delete the raw SQL:

- [ ] `cmd/domonda-cron-scheduler/cron/config.go` → `DeleteFinishedJobs(ctx, time.Hour)`
- [ ] `pkg/apis/gmailsync/job.go` → `HasJobWithTypeAndPayload`
- [ ] `pkg/dashboard/jobqueue.go` → `GetJobs` + `GetJobStatusCounts`
- [ ] `pkg/banking/matching/job.go` → debounce method
