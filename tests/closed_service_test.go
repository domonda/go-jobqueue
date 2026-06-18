package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
)

// TestServiceMethodsAfterClose verifies that every DB-backed method guarded by
// the closed flag returns jobqueue.ErrClosed once the service is closed, instead
// of touching the (now unusable) connection.
func TestServiceMethodsAfterClose(t *testing.T) {
	_ = jobqueue.Close()
	setupDBConn(t)
	t.Cleanup(func() { _ = jobqueue.Close() })
	ctx := t.Context()

	dbAPI := dataBaseAPI(t, ctx)

	// Build the argument values before closing (these constructors do not touch
	// the database).
	id := uu.IDFrom("e3a10000-0000-4000-8000-000000000001")
	job, err := jobqueue.NewJob(id, "test-closed-type", "test-closed", "{}", nullable.Time{})
	require.NoError(t, err)
	bundle, err := jobqueue.NewJobBundle(ctx, "test-closed-bundle", "test-closed",
		[]jobqueue.JobDesc{{Type: "test-closed-type", Payload: "{}", Origin: "test-closed"}}, nullable.Time{})
	require.NoError(t, err)

	calls := []struct {
		name string
		fn   func() error
	}{
		{"AddJob", func() error { return dbAPI.AddJob(ctx, job) }},
		{"AddJobBundle", func() error { return dbAPI.AddJobBundle(ctx, bundle) }},
		{"GetStatus", func() error { _, e := dbAPI.GetStatus(ctx); return e }},
		{"GetAllJobsToDo", func() error { _, e := dbAPI.GetAllJobsToDo(ctx); return e }},
		{"GetAllJobsStartedBefore", func() error { _, e := dbAPI.GetAllJobsStartedBefore(ctx, time.Now()); return e }},
		{"GetAllJobsWithErrors", func() error { _, e := dbAPI.GetAllJobsWithErrors(ctx); return e }},
		{"GetJob", func() error { _, e := dbAPI.GetJob(ctx, id); return e }},
		{"GetJobBundle", func() error { _, e := dbAPI.GetJobBundle(ctx, id); return e }},
		{"StartNextJobOrNil", func() error { _, e := dbAPI.StartNextJobOrNil(ctx); return e }},
		{"SetJobError", func() error { return dbAPI.SetJobError(ctx, id, "boom", nullable.JSON{}) }},
		{"SetJobResult", func() error { return dbAPI.SetJobResult(ctx, id, nullable.JSON{}) }},
		{"SetJobStart", func() error { return dbAPI.SetJobStart(ctx, id, time.Now()) }},
		{"SetJobWorkerAlive", func() error { return dbAPI.SetJobWorkerAlive(ctx, id) }},
		{"ScheduleRetry", func() error { return dbAPI.ScheduleRetry(ctx, id, time.Now(), 1) }},
		{"ResetJob", func() error { return dbAPI.ResetJob(ctx, id) }},
		{"ResetJobs", func() error { return dbAPI.ResetJobs(ctx, uu.IDSlice{id}) }},
		{"DeleteJob", func() error { return dbAPI.DeleteJob(ctx, id) }},
		{"DeleteFinishedJobs", func() error { return dbAPI.DeleteFinishedJobs(ctx) }},
		{"DeleteJobsFromOrigin", func() error { return dbAPI.DeleteJobsFromOrigin(ctx, "test-closed") }},
		{"DeleteJobsOfType", func() error { return dbAPI.DeleteJobsOfType(ctx, "test-closed-type") }},
		{"DeleteJobBundle", func() error { return dbAPI.DeleteJobBundle(ctx, id) }},
		{"DeleteJobBundlesFromOrigin", func() error { return dbAPI.DeleteJobBundlesFromOrigin(ctx, "test-closed") }},
		{"DeleteJobBundlesOfType", func() error { return dbAPI.DeleteJobBundlesOfType(ctx, "test-closed-bundle") }},
		{"DeleteAllJobsAndBundles", func() error { return jobworker.DeleteAllJobsAndBundles(ctx) }},
	}

	require.NoError(t, jobqueue.Close(), "first Close succeeds")

	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			require.ErrorIs(t, c.fn(), jobqueue.ErrClosed)
		})
	}
}
