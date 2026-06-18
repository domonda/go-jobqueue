package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
)

// insertTestJob inserts a standalone worker.job row with explicit state, for the
// query- and delete-method tests. Pass nil for a NULL started_at/stopped_at, and
// nil or a string for error_msg. payload, priority and retry counts are fixed.
func insertTestJob(t *testing.T, ctx context.Context, id uu.ID, jobType, origin string, startedAt, stoppedAt, errorMsg any) {
	t.Helper()
	err := db.Exec(ctx,
		/*sql*/ `
			insert into worker.job (
				id, type, payload, priority, origin,
				max_retry_count, current_retry_count,
				started_at, stopped_at, error_msg
			) values ($1, $2, '{}'::jsonb, 0, $3, 0, 0, $4, $5, $6)
		`,
		id, jobType, origin, startedAt, stoppedAt, errorMsg,
	)
	require.NoError(t, err)
}

// insertTestBundle inserts an empty worker.job_bundle row.
func insertTestBundle(t *testing.T, ctx context.Context, id uu.ID, bundleType, origin string) {
	t.Helper()
	err := db.Exec(ctx,
		/*sql*/ `insert into worker.job_bundle (id, type, origin, num_jobs) values ($1, $2, $3, 0)`,
		id, bundleType, origin,
	)
	require.NoError(t, err)
}

// insertTestBundledJob inserts a worker.job that belongs to bundleID (so the
// ON DELETE CASCADE from worker.job_bundle applies).
func insertTestBundledJob(t *testing.T, ctx context.Context, id, bundleID uu.ID, jobType, origin string, stoppedAt any) {
	t.Helper()
	err := db.Exec(ctx,
		/*sql*/ `
			insert into worker.job (id, bundle_id, type, payload, priority, origin, stopped_at)
			values ($1, $2, $3, '{}'::jsonb, 0, $4, $5)
		`,
		id, bundleID, jobType, origin, stoppedAt,
	)
	require.NoError(t, err)
}

// countRows runs a `select count(*) ...` query and returns the scalar count.
func countRows(t *testing.T, ctx context.Context, query string, args ...any) int {
	t.Helper()
	n, err := db.QueryRowAs[int](ctx, query, args...)
	require.NoError(t, err)
	return n
}

// containsJobID reports whether jobs contains a job with the given ID.
func containsJobID(jobs []*jobqueue.Job, id uu.ID) bool {
	for _, j := range jobs {
		if j.ID == id {
			return true
		}
	}
	return false
}

func TestGetStatus(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

	const origin = "test-get-status"
	t.Cleanup(func() {
		bg := context.Background()
		_ = db.Exec(bg, `delete from worker.job where origin = $1`, origin)
		_ = db.Exec(bg, `delete from worker.job_bundle where origin = $1`, origin)
	})

	// Assert on the delta rather than absolute counts so the test is independent
	// of any rows other tests may have left behind.
	before, err := jobqueue.GetStatus(ctx)
	require.NoError(t, err)

	insertTestJob(t, ctx, uu.IDFrom("e1a10000-0000-4000-8000-000000000001"), "test-get-status-type", origin, nil, nil, nil)
	insertTestJob(t, ctx, uu.IDFrom("e1a10000-0000-4000-8000-000000000002"), "test-get-status-type", origin, nil, nil, nil)
	insertTestBundle(t, ctx, uu.IDFrom("e1b10000-0000-4000-8000-000000000001"), "test-get-status-bundle", origin)

	after, err := jobqueue.GetStatus(ctx)
	require.NoError(t, err)

	assert.Equal(t, before.NumJobs+2, after.NumJobs, "two jobs were added")
	assert.Equal(t, before.NumJobBundles+1, after.NumJobBundles, "one bundle was added")
}

func TestGetAllJobsToDo(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

	const origin = "test-get-jobs-todo"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin = $1`, origin)
	})

	todoID := uu.IDFrom("e1a20000-0000-4000-8000-000000000001")
	stoppedID := uu.IDFrom("e1a20000-0000-4000-8000-000000000002")
	insertTestJob(t, ctx, todoID, "test-get-jobs-todo-type", origin, nil, nil, nil)             // open
	insertTestJob(t, ctx, stoppedID, "test-get-jobs-todo-type", origin, time.Now(), time.Now(), nil) // stopped

	jobs, err := jobqueue.GetAllJobsToDo(ctx)
	require.NoError(t, err)
	assert.True(t, containsJobID(jobs, todoID), "an unstopped job must be returned")
	assert.False(t, containsJobID(jobs, stoppedID), "a stopped job must be excluded")
}

func TestGetAllJobsStartedBefore(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

	const origin = "test-get-jobs-started-before"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin = $1`, origin)
	})

	now := time.Now()
	startedPastID := uu.IDFrom("e1a30000-0000-4000-8000-000000000001")    // started in the past, not stopped
	notStartedID := uu.IDFrom("e1a30000-0000-4000-8000-000000000002")     // never started
	startedStoppedID := uu.IDFrom("e1a30000-0000-4000-8000-000000000003") // started in the past, but stopped
	startedFutureID := uu.IDFrom("e1a30000-0000-4000-8000-000000000004")  // started after the cutoff

	insertTestJob(t, ctx, startedPastID, "test-started-before-type", origin, now.Add(-time.Hour), nil, nil)
	insertTestJob(t, ctx, notStartedID, "test-started-before-type", origin, nil, nil, nil)
	insertTestJob(t, ctx, startedStoppedID, "test-started-before-type", origin, now.Add(-time.Hour), now.Add(-30*time.Minute), nil)
	insertTestJob(t, ctx, startedFutureID, "test-started-before-type", origin, now.Add(time.Hour), nil, nil)

	jobs, err := jobqueue.GetAllJobsStartedBefore(ctx, now)
	require.NoError(t, err)
	assert.True(t, containsJobID(jobs, startedPastID), "started-and-running job before the cutoff is returned")
	assert.False(t, containsJobID(jobs, notStartedID), "a never-started job is excluded")
	assert.False(t, containsJobID(jobs, startedStoppedID), "a stopped job is excluded")
	assert.False(t, containsJobID(jobs, startedFutureID), "a job started after the cutoff is excluded")
}

func TestGetAllJobsWithErrors(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

	const origin = "test-get-jobs-with-errors"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin = $1`, origin)
	})

	erroredID := uu.IDFrom("e1a40000-0000-4000-8000-000000000001")
	okID := uu.IDFrom("e1a40000-0000-4000-8000-000000000002")
	insertTestJob(t, ctx, erroredID, "test-errors-type", origin, time.Now(), time.Now(), "boom")
	insertTestJob(t, ctx, okID, "test-errors-type", origin, time.Now(), time.Now(), nil)

	jobs, err := jobqueue.GetAllJobsWithErrors(ctx)
	require.NoError(t, err)
	assert.True(t, containsJobID(jobs, erroredID), "a job with error_msg must be returned")
	assert.False(t, containsJobID(jobs, okID), "a job without error_msg must be excluded")
}
