CREATE FUNCTION worker.job_available() RETURNS trigger AS
$$
BEGIN
    PERFORM pg_notify('job_available',
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

CREATE TRIGGER job_available_insert_trigger
    AFTER INSERT ON worker.job
    FOR EACH ROW
    WHEN (
        (
            -- the `start_at` does not exist
            NEW.start_at IS NULL
        ) OR (
            -- the `start_at` is in the present or the past
            now() >= NEW.start_at
        )
    )
    EXECUTE PROCEDURE worker.job_available();

-- Fires when a job is reset (started_at cleared) and is available now.
-- This covers ResetJob, ResetJobs, SetJobStart, and ScheduleRetry
-- which UPDATE the row instead of INSERTing.
CREATE TRIGGER job_available_update_trigger
    AFTER UPDATE ON worker.job
    FOR EACH ROW
    WHEN (
        (
            -- was previously started
            OLD.started_at IS NOT NULL
        ) AND (
            -- is now reset (available for pickup)
            NEW.started_at IS NULL
        ) AND (
            -- the `start_at` does not exist
            NEW.start_at IS NULL
            OR
            -- the `start_at` is in the present or the past
            now() >= NEW.start_at
        )
    )
    EXECUTE PROCEDURE worker.job_available();

----

CREATE FUNCTION worker.job_stopped() RETURNS trigger AS
$$
BEGIN
    PERFORM pg_notify('job_stopped',
        json_build_object(
            'id',        NEW.id,
            'type',      NEW."type",
            'origin',    NEW.origin,
            'willRetry', NEW.error_msg IS NOT NULL AND NEW.current_retry_count < NEW.max_retry_count
        )::text
    );
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER job_stopped_trigger
    AFTER UPDATE ON worker.job
    FOR EACH ROW
    WHEN (
        (
            OLD.stopped_at IS NULL
        ) AND (
            NEW.stopped_at IS NOT NULL
        )
    )
    EXECUTE PROCEDURE worker.job_stopped();
