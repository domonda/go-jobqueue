package tests

import (
	"context"
	"testing"
	"time"

	"github.com/domonda/go-sqldb/db"
	"github.com/domonda/go-types/uu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
)

func countJobByID(t *testing.T, id uu.ID) int {
	t.Helper()
	return countRows(t, `select count(*) from worker.job where id = $1`, id)
}

func countBundleByID(t *testing.T, id uu.ID) int {
	t.Helper()
	return countRows(t, `select count(*) from worker.job_bundle where id = $1`, id)
}

func TestDeleteFinishedJobs(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	const origin = "test-delete-finished"
	t.Cleanup(func() {
		bg := context.Background()
		_ = db.Exec(bg, `delete from worker.job where origin = $1`, origin)
		_ = db.Exec(bg, `delete from worker.job_bundle where origin = $1`, origin)
	})

	finishedID := uu.IDFrom("e2a10000-0000-4000-8000-000000000001") // stopped, no error, standalone -> deleted
	erroredID := uu.IDFrom("e2a10000-0000-4000-8000-000000000002")  // stopped with error -> kept
	openID := uu.IDFrom("e2a10000-0000-4000-8000-000000000003")     // not stopped -> kept
	bundleID := uu.IDFrom("e2b10000-0000-4000-8000-000000000001")   // bundle for the bundled job
	bundledID := uu.IDFrom("e2a10000-0000-4000-8000-000000000004")  // stopped, no error, but in a bundle -> kept

	insertTestJob(t, finishedID, "test-delete-finished-type", origin, time.Now(), time.Now(), nil)
	insertTestJob(t, erroredID, "test-delete-finished-type", origin, time.Now(), time.Now(), "boom")
	insertTestJob(t, openID, "test-delete-finished-type", origin, nil, nil, nil)
	insertTestBundle(t, bundleID, "test-delete-finished-bundle", origin)
	insertTestBundledJob(t, bundledID, bundleID, "test-delete-finished-type", origin, time.Now())

	require.NoError(t, jobqueue.DeleteFinishedJobs(t.Context()))

	assert.Equal(t, 0, countJobByID(t, finishedID), "a finished standalone job is deleted")
	assert.Equal(t, 1, countJobByID(t, erroredID), "a job with an error is kept")
	assert.Equal(t, 1, countJobByID(t, openID), "an unstopped job is kept")
	assert.Equal(t, 1, countJobByID(t, bundledID), "a finished job in a bundle is kept")
}

func TestDeleteJobsFromOrigin(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	const originA = "test-delete-jobs-origin-a"
	const originB = "test-delete-jobs-origin-b"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin in ($1, $2)`, originA, originB)
	})

	jobA1 := uu.IDFrom("e2a20000-0000-4000-8000-000000000001")
	jobA2 := uu.IDFrom("e2a20000-0000-4000-8000-000000000002")
	jobB1 := uu.IDFrom("e2a20000-0000-4000-8000-000000000003")
	insertTestJob(t, jobA1, "test-delete-origin-type", originA, nil, nil, nil)
	insertTestJob(t, jobA2, "test-delete-origin-type", originA, nil, nil, nil)
	insertTestJob(t, jobB1, "test-delete-origin-type", originB, nil, nil, nil)

	dbAPI := dataBaseAPI(t)
	require.NoError(t, dbAPI.DeleteJobsFromOrigin(t.Context(), originA))

	assert.Equal(t, 0, countRows(t, `select count(*) from worker.job where origin = $1`, originA), "all originA jobs are deleted")
	assert.Equal(t, 1, countRows(t, `select count(*) from worker.job where origin = $1`, originB), "originB jobs are untouched")
}

func TestDeleteJobsOfType(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	const origin = "test-delete-jobs-of-type"
	const typeA = "test-delete-of-type-a"
	const typeB = "test-delete-of-type-b"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job where origin = $1`, origin)
	})

	insertTestJob(t, uu.IDFrom("e2a30000-0000-4000-8000-000000000001"), typeA, origin, nil, nil, nil)
	insertTestJob(t, uu.IDFrom("e2a30000-0000-4000-8000-000000000002"), typeA, origin, nil, nil, nil)
	insertTestJob(t, uu.IDFrom("e2a30000-0000-4000-8000-000000000003"), typeB, origin, nil, nil, nil)

	dbAPI := dataBaseAPI(t)
	require.NoError(t, dbAPI.DeleteJobsOfType(t.Context(), typeA))

	assert.Equal(t, 0, countRows(t, `select count(*) from worker.job where type = $1`, typeA), "all typeA jobs are deleted")
	assert.Equal(t, 1, countRows(t, `select count(*) from worker.job where type = $1`, typeB), "typeB jobs are untouched")
}

func TestDeleteJobBundlesFromOrigin(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	const originA = "test-delete-bundles-origin-a"
	const originB = "test-delete-bundles-origin-b"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job_bundle where origin in ($1, $2)`, originA, originB)
	})

	bundleA := uu.IDFrom("e2b20000-0000-4000-8000-000000000001")
	bundleB := uu.IDFrom("e2b20000-0000-4000-8000-000000000002")
	cascadedJob := uu.IDFrom("e2a40000-0000-4000-8000-000000000001")
	insertTestBundle(t, bundleA, "test-delete-bundles-type", originA)
	insertTestBundle(t, bundleB, "test-delete-bundles-type", originB)
	insertTestBundledJob(t, cascadedJob, bundleA, "test-delete-bundles-job-type", originA, nil)

	dbAPI := dataBaseAPI(t)
	require.NoError(t, dbAPI.DeleteJobBundlesFromOrigin(t.Context(), originA))

	assert.Equal(t, 0, countBundleByID(t, bundleA), "originA bundle is deleted")
	assert.Equal(t, 1, countBundleByID(t, bundleB), "originB bundle is untouched")
	assert.Equal(t, 0, countJobByID(t, cascadedJob), "the deleted bundle's jobs are cascaded away")
}

func TestDeleteJobBundlesOfType(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	const origin = "test-delete-bundles-of-type"
	const typeA = "test-delete-bundle-of-type-a"
	const typeB = "test-delete-bundle-of-type-b"
	t.Cleanup(func() {
		_ = db.Exec(context.Background(), `delete from worker.job_bundle where origin = $1`, origin)
	})

	bundleA := uu.IDFrom("e2b30000-0000-4000-8000-000000000001")
	bundleB := uu.IDFrom("e2b30000-0000-4000-8000-000000000002")
	insertTestBundle(t, bundleA, typeA, origin)
	insertTestBundle(t, bundleB, typeB, origin)

	dbAPI := dataBaseAPI(t)
	require.NoError(t, dbAPI.DeleteJobBundlesOfType(t.Context(), typeA))

	assert.Equal(t, 0, countRows(t, `select count(*) from worker.job_bundle where type = $1`, typeA), "typeA bundles are deleted")
	assert.Equal(t, 1, countRows(t, `select count(*) from worker.job_bundle where type = $1`, typeB), "typeB bundles are untouched")
}

func TestDeleteAllJobsAndBundles(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })

	const origin = "test-delete-all"
	t.Cleanup(func() {
		bg := context.Background()
		_ = db.Exec(bg, `delete from worker.job where origin = $1`, origin)
		_ = db.Exec(bg, `delete from worker.job_bundle where origin = $1`, origin)
	})

	bundleID := uu.IDFrom("e2b40000-0000-4000-8000-000000000001")
	insertTestJob(t, uu.IDFrom("e2a50000-0000-4000-8000-000000000001"), "test-delete-all-type", origin, nil, nil, nil)
	insertTestBundle(t, bundleID, "test-delete-all-bundle", origin)
	insertTestBundledJob(t, uu.IDFrom("e2a50000-0000-4000-8000-000000000002"), bundleID, "test-delete-all-type", origin, nil)

	require.NoError(t, jobworker.DeleteAllJobsAndBundles(t.Context()))

	assert.Equal(t, 0, countRows(t, `select count(*) from worker.job`), "all jobs are deleted")
	assert.Equal(t, 0, countRows(t, `select count(*) from worker.job_bundle`), "all bundles are deleted")
}
