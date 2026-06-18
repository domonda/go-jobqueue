create table worker.job (
    id uuid primary key default uuid_generate_v4(),

    bundle_id uuid references worker.job_bundle(id) on delete cascade,

    "type"   text not null check(length("type") > 0 and length("type") <= 100),
    payload  jsonb not null,
    priority bigint not null,
    origin   text not null check(length(origin) > 0 and length(origin) <= 100),

    max_retry_count     int not null default 0,
    current_retry_count int not null default 0,
    start_at            timestamptz, -- If NOT NULL, earliest time to start the job

    started_at      timestamptz, -- Time when started working on the job, or NULL when not started
    worker_alive_at timestamptz, -- Heartbeat updated periodically while a worker processes the job; NULL when not being processed. A stale value while stopped_at IS NULL indicates the worker crashed.
    stopped_at      timestamptz, -- Time when working on job was stopped for any reason

    error_msg  text,  -- If there was an error working off the job
    error_data jsonb, -- Optional error metadata
	result     jsonb, -- Result if the job returned one

    updated_at timestamptz not null default now(),
    created_at timestamptz not null default now()
);

comment on table worker.job IS 'A `Job` to be worked out later.';

-- Partial index on the bundle_id foreign key: serves GetJobBundle's
-- `where bundle_id = $1` lookups and, more importantly, the ON DELETE CASCADE
-- from worker.job_bundle (Postgres does not auto-index referencing columns, so
-- without this every bundle delete seq-scans worker.job). Partial because
-- bundle_id is NULL for all standalone jobs, which never participate in either.
create index worker_job_bundle_id_idx  on worker.job(bundle_id) where bundle_id is not null;
create index worker_job_type_idx       on worker.job("type");
create index worker_job_start_at_idx   on worker.job(start_at);
create index worker_job_started_at_idx on worker.job(started_at);
create index worker_job_stopped_at_idx on worker.job(stopped_at);
-- Partial index backing StartNextJobOrNil, the hottest query in the queue: every
-- worker poll runs it, with concurrent FOR UPDATE SKIP LOCKED contention. The
-- claim selects the next unstarted job of a registered type and marks it started:
--   where started_at is null and (start_at is null or start_at <= now())
--     and "type" in (...) order by priority desc, created_at asc limit 1
--     for update skip locked
-- Column choices:
--   * `where started_at is null` confines the index to the pending backlog, so it
--     stays small as finished jobs accumulate instead of scanning every row of a
--     type and filtering the already-started ones out.
--   * `"type"` leading prunes to the registered types in the `in (...)` list, so
--     disjoint worker fleets sharing one database skip each other's jobs.
--   * `priority desc, created_at asc` matches the ORDER BY exactly (mixed
--     directions): for a single registered type `limit 1` stops at the first
--     unlocked match with no sort; for several types Postgres merges the per-type
--     index streams in claim order instead of sorting the whole backlog.
create index worker_job_claim_idx on worker.job("type", priority desc, created_at asc)
  where started_at is null;
-- worker_alive_at is intentionally NOT indexed: the heartbeat rewrites it every
-- HeartbeatInterval for each in-progress job, and indexing it would defeat
-- Postgres HOT updates (an indexed column changing forces a new index entry
-- every heartbeat) and bloat the index. Its only reader is the startup reaper
-- (resetInterruptedRetryableJobs), a rare query that tolerates a scan.

