\echo
\echo '=== schema/worker.sql ==='
\echo

BEGIN;
CREATE EXTENSION "uuid-ossp";
CREATE SCHEMA worker;
\ir worker/job_bundle.sql
\ir worker/job.sql
\ir worker/job_triggers.sql

COMMIT;