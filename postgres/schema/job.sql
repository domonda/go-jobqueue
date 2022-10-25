CREATE TABLE worker.job (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),

    bundle_id uuid REFERENCES worker.job_bundle(id) ON DELETE CASCADE,

    "type"   text   NOT NULL CHECK(length("type") > 0 AND length("type") <= 100),
    payload  jsonb  NOT NULL,
    priority bigint NOT NULL,
    origin   text   NOT NULL CHECK(length(origin) > 0 AND length(origin) <= 100),

    start_at   timestamptz, -- If NOT NULL, earliest time to start the job
    started_at timestamptz, -- Time when started working on the job, or NULL when not started
    stopped_at timestamptz, -- Time when working on job was stopped for any reason

    -- issue_type text,  -- Type of the issue that has to be resolved before the job can continue
    -- issue_data jsonb, -- Data about the issue that needs to be resolved

    error_msg  text,  -- If there was an error working off the job
    error_data jsonb, -- Optional error metadata
	result     jsonb, -- Result if the job returned one

    updated_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE worker.job IS 'A `Job` to be worked out later.';

CREATE INDEX worker_job_type_idx       ON worker.job("type");
CREATE INDEX worker_job_start_at_idx   ON worker.job(start_at);
CREATE INDEX worker_job_started_at_idx ON worker.job(started_at);
CREATE INDEX worker_job_stopped_at_idx ON worker.job(stopped_at);

-- Indices removed because payload may get larger than max 2712 bytes:
-- CREATE INDEX worker_job_payload_idx ON worker.job (payload);
-- CREATE INDEX worker_job_type_payload_start_at_idx ON worker.job ("type", payload, start_at);

