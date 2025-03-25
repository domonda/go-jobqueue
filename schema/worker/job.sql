create table worker.job (
    id uuid primary key default uuid_generate_v4(),

    bundle_id uuid references worker.job_bundle(id) on delete cascade,

    "type"   text not null check(length("type") > 0 and length("type") <= 100),
    payload  jsonb not null,
    priority bigint not null,
    origin   text not null check(length(origin) > 0 and length(origin) <= 100),
    retry_count int not null default 0,

    start_at   timestamptz, -- If NOT NULL, earliest time to start the job
    started_at timestamptz, -- Time when started working on the job, or NULL when not started
    stopped_at timestamptz, -- Time when working on job was stopped for any reason

    -- issue_type text,  -- Type of the issue that has to be resolved before the job can continue
    -- issue_data jsonb, -- Data about the issue that needs to be resolved

    error_msg  text,  -- If there was an error working off the job
    error_data jsonb, -- Optional error metadata
	result     jsonb, -- Result if the job returned one

    updated_at timestamptz not null default current_timestamp,
    created_at timestamptz not null default current_timestamp
);

comment on table worker.job IS 'A `Job` to be worked out later.';

create index worker_job_type_idx     on worker.job("type");
create index worker_job_start_at_idx on worker.job(start_at);
create index worker_job_started_at_idx on worker.job(started_at);
create index worker_job_stopped_at_idx on worker.job(stopped_at);

-- Indices removed because payload may get larger than max 2712 bytes:
-- CREATE INDEX worker_job_payload_idx ON worker.job (payload);
-- CREATE INDEX worker_job_type_payload_start_at_idx ON worker.job ("type", payload, start_at);

----

-- TODO remove
create function worker.job_ready(
    job worker.job
) returns boolean as
$$
    select (
        -- not started
		job.started_at is null
	) and (
		-- start at time is reached
		(job.start_at is null) or (job.start_at <= now())
	)
$$
language sql immutable;
