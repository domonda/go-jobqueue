CREATE TABLE worker.job_bundle (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),

    "type" text NOT NULL CHECK(length("type") > 0),
    origin text NOT NULL CHECK(length(origin) > 0),

    num_jobs         bigint NOT NULL CHECK(num_jobs >= 0),
    num_jobs_stopped bigint NOT NULL DEFAULT 0 CHECK(num_jobs_stopped >= 0 AND num_jobs_stopped <= num_jobs),

    updated_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE worker.job_bundle IS 'A bundle of `Job`s to be worked out later.';

CREATE INDEX worker_job_bundle_type_idx ON worker.job_bundle ("type");

----

CREATE FUNCTION worker.job_bundle_stopped() RETURNS trigger AS
$$
BEGIN
    PERFORM pg_notify('job_bundle_stopped', 
        json_build_object(
            'id',     NEW.id,
            'type',   NEW."type",
            'origin', NEW.origin
        )::text
    );
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER job_bundle_stopped_trigger
    AFTER UPDATE ON worker.job_bundle
    FOR EACH ROW
    WHEN NEW.num_jobs_stopped = NEW.num_jobs
        AND OLD.num_jobs_stopped < OLD.num_jobs
    EXECUTE PROCEDURE worker.job_bundle_stopped();

