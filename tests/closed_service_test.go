package tests

import (
	"testing"
	"time"

	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"
	"github.com/stretchr/testify/require"

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

	dbAPI := dataBaseAPI(t)

	// Build the argument values before closing (these constructors do not touch
	// the database).
	id := uu.IDFrom("e3a10000-0000-4000-8000-000000000001")
	job, err := jobqueue.NewJob(id, "test-closed-type", "test-closed", "{}", nullable.Time{})
	require.NoError(t, err)
	bundle, err := jobqueue.NewJobBundle(t.Context(), "test-closed-bundle", "test-closed",
		[]jobqueue.JobDesc{{Type: "test-closed-type", Payload: "{}", Origin: "test-closed"}}, nullable.Time{})
	require.NoError(t, err)

	calls := []struct {
		name string
		fn   func() error
	}{
		{"AddJob", func() error { return dbAPI.AddJob(t.Context(), job) }},
		{"AddJobBundle", func() error { return dbAPI.AddJobBundle(t.Context(), bundle) }},
		{"GetStatus", func() error { _, e := dbAPI.GetStatus(t.Context()); return e }},
		{"GetAllJobsToDo", func() error { _, e := dbAPI.GetAllJobsToDo(t.Context()); return e }},
		{"GetAllJobsStartedBefore", func() error { _, e := dbAPI.GetAllJobsStartedBefore(t.Context(), time.Now()); return e }},
		{"GetAllJobsWithErrors", func() error { _, e := dbAPI.GetAllJobsWithErrors(t.Context()); return e }},
		{"GetJob", func() error { _, e := dbAPI.GetJob(t.Context(), id); return e }},
		{"GetJobBundle", func() error { _, e := dbAPI.GetJobBundle(t.Context(), id); return e }},
		{"StartNextJobOrNil", func() error { _, e := dbAPI.StartNextJobOrNil(t.Context()); return e }},
		{"SetJobError", func() error { return dbAPI.SetJobError(t.Context(), id, "boom", nullable.JSON{}) }},
		{"SetJobResult", func() error { return dbAPI.SetJobResult(t.Context(), id, nullable.JSON{}) }},
		{"SetJobStart", func() error { return dbAPI.SetJobStart(t.Context(), id, time.Now()) }},
		{"SetJobWorkerAlive", func() error { return dbAPI.SetJobWorkerAlive(t.Context(), id) }},
		{"ScheduleRetry", func() error { return dbAPI.ScheduleRetry(t.Context(), id, time.Now(), 1) }},
		{"ResetJob", func() error { return dbAPI.ResetJob(t.Context(), id) }},
		{"ResetJobs", func() error { return dbAPI.ResetJobs(t.Context(), uu.IDSlice{id}) }},
		{"DeleteJob", func() error { return dbAPI.DeleteJob(t.Context(), id) }},
		{"DeleteFinishedJobs", func() error { return dbAPI.DeleteFinishedJobs(t.Context()) }},
		{"DeleteJobsFromOrigin", func() error { return dbAPI.DeleteJobsFromOrigin(t.Context(), "test-closed") }},
		{"DeleteJobsOfType", func() error { return dbAPI.DeleteJobsOfType(t.Context(), "test-closed-type") }},
		{"DeleteJobBundle", func() error { return dbAPI.DeleteJobBundle(t.Context(), id) }},
		{"DeleteJobBundlesFromOrigin", func() error { return dbAPI.DeleteJobBundlesFromOrigin(t.Context(), "test-closed") }},
		{"DeleteJobBundlesOfType", func() error { return dbAPI.DeleteJobBundlesOfType(t.Context(), "test-closed-bundle") }},
		{"DeleteAllJobsAndBundles", func() error { return jobworker.DeleteAllJobsAndBundles(t.Context()) }},
	}

	require.NoError(t, jobqueue.Close(), "first Close succeeds")

	for _, c := range calls {
		t.Run(c.name, func(t *testing.T) {
			require.ErrorIs(t, c.fn(), jobqueue.ErrClosed)
		})
	}
}
