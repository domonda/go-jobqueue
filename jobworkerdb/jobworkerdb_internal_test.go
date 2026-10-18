package jobworkerdb

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-sqldb/pqconn"
	"github.com/domonda/go-types/notnull"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
)

// TestBuildClaimJobQuery verifies the StartNextJobOrNil claim query is assembled
// correctly with the registered job types inlined as SQL literals.
//
// It uses pqconn.QueryFormatter{} on purpose: that is the same QueryFormatter the
// production claim path reaches via db.Conn(ctx), so the literals are escaped here
// exactly as they will be at runtime. This is a pure string test and needs no
// database.
func TestBuildClaimJobQuery(t *testing.T) {
	formatter := pqconn.QueryFormatter{}

	t.Run("single job type", func(t *testing.T) {
		query := buildClaimJobQuery([]string{"email"}, formatter)

		// The static skeleton of the combined claim+update statement.
		assert.Contains(t, query, "with claimed as (")
		assert.Contains(t, query, "select id")
		assert.Contains(t, query, "from worker.job")
		assert.Contains(t, query, "where started_at is null")
		assert.Contains(t, query, "start_at <= now()")
		assert.Contains(t, query, `and "type" in ('email')`)
		assert.Contains(t, query, "order by")
		assert.Contains(t, query, "priority desc")
		assert.Contains(t, query, "created_at asc")
		assert.Contains(t, query, "limit 1")
		assert.Contains(t, query, "for update skip locked")
		assert.Contains(t, query, "update worker.job")
		assert.Contains(t, query, "returning worker.job.*")

		// The statement takes no bind parameters: job types are inlined and all
		// timestamps use now(), so it can be cached as a prepared statement.
		assert.NotContains(t, query, "$1")
		assert.NotContains(t, query, "$2")
	})

	t.Run("multiple job types are comma joined in slice order", func(t *testing.T) {
		query := buildClaimJobQuery([]string{"a", "b", "c"}, formatter)
		assert.Contains(t, query, `and "type" in ('a','b','c')`)
	})

	t.Run("single quotes are doubled so a payload cannot break out", func(t *testing.T) {
		jobType := `weird'); drop table worker.job; --`
		query := buildClaimJobQuery([]string{jobType}, formatter)

		// The inlined literal must equal the formatter's quoted form, which doubles
		// the single quote and keeps the whole payload inside one string literal.
		want := formatter.FormatStringLiteral(jobType)
		assert.Contains(t, query, "in ("+want+")")
		assert.Contains(t, want, "''", "single quote doubled by the formatter")
	})

	t.Run("backslashes switch to C-style E'' escaping", func(t *testing.T) {
		jobType := `back\slash`
		query := buildClaimJobQuery([]string{jobType}, formatter)

		want := formatter.FormatStringLiteral(jobType)
		assert.Contains(t, query, "in ("+want+")")
		assert.Contains(t, want, `E'`, "pq.QuoteLiteral uses E'...' when a backslash is present")
	})

	t.Run("inlined literals match formatter output exactly", func(t *testing.T) {
		jobTypes := []string{"plain", "with'quote", `with\backslash`}
		query := buildClaimJobQuery(jobTypes, formatter)

		quoted := make([]string, len(jobTypes))
		for i, jt := range jobTypes {
			quoted[i] = formatter.FormatStringLiteral(jt)
		}
		assert.Contains(t, query, "in ("+strings.Join(quoted, ",")+")")
	})
}

// TestAddNilJob pins that AddJob and AddJobBundle return an error instead of
// panicking for a nil job or bundle, as passed by a caller that discarded the
// error of NewJob rejecting a payload. Both return before touching the database.
func TestAddNilJob(t *testing.T) {
	var service jobworkerDB

	t.Run("nil job", func(t *testing.T) {
		var err error
		require.NotPanics(t, func() { err = service.AddJob(t.Context(), nil) })
		require.ErrorContains(t, err, "nil job")
	})

	t.Run("nil job bundle", func(t *testing.T) {
		var err error
		require.NotPanics(t, func() { err = service.AddJobBundle(t.Context(), nil) })
		require.ErrorContains(t, err, "nil job bundle")
	})

	t.Run("nil job in a bundle", func(t *testing.T) {
		bundle := &jobqueue.JobBundle{
			ID:      uu.IDFrom("c2a7c5b4-4b0c-4b3e-9a5a-1f2e3d4c5b6a"),
			Type:    "bundle-type",
			Origin:  "origin",
			NumJobs: 2,
			Jobs:    []*jobqueue.Job{{Type: "job-type", Origin: "origin", Payload: notnull.JSON(`{}`)}, nil},
		}
		var err error
		require.NotPanics(t, func() { err = service.AddJobBundle(t.Context(), bundle) })
		require.ErrorContains(t, err, "nil job at index 1")
		assert.True(t, bundle.Jobs[0].BundleID.IsNull(), "a rejected bundle must not modify its jobs")
	})
}

// TestAddJobBundleRejectedLeavesJobsUnmodified pins that a bundle rejected for a
// payload, or because the service is closed, doesn't set its jobs' BundleID:
// a job pointing at a bundle that was never inserted fails a later Add with a
// foreign key violation.
func TestAddJobBundleRejectedLeavesJobsUnmodified(t *testing.T) {
	newBundle := func() *jobqueue.JobBundle {
		return &jobqueue.JobBundle{
			ID:      uu.IDFrom("c2a7c5b4-4b0c-4b3e-9a5a-1f2e3d4c5b6b"),
			Type:    "bundle-type",
			Origin:  "origin",
			NumJobs: 2,
			Jobs: []*jobqueue.Job{
				{Type: "job-type", Origin: "origin", Payload: notnull.JSON(`{}`)},
				{Type: "job-type", Origin: "origin", Payload: notnull.JSON(`{invalid`)},
			},
		}
	}

	t.Run("invalid payload", func(t *testing.T) {
		var service jobworkerDB
		bundle := newBundle()
		require.ErrorIs(t, service.AddJobBundle(t.Context(), bundle), jobqueue.ErrInvalidPayload)
		assert.True(t, bundle.Jobs[0].BundleID.IsNull())
	})

	t.Run("closed service", func(t *testing.T) {
		var service jobworkerDB
		service.closed.Store(true)
		bundle := newBundle()
		require.ErrorIs(t, service.AddJobBundle(t.Context(), bundle), jobqueue.ErrClosed)
		assert.True(t, bundle.Jobs[0].BundleID.IsNull())
	})
}

// TestAddJobBundleStampsBundleIDBeforeIgnoreAndSync pins that ignore callbacks
// and synchronous workers see the bundle's ID on its jobs, as they did before
// the stamping moved behind the payload check.
func TestAddJobBundleStampsBundleIDBeforeIgnoreAndSync(t *testing.T) {
	bundleID := uu.IDFrom("c2a7c5b4-4b0c-4b3e-9a5a-1f2e3d4c5b6e")
	newBundle := func(jobType string) *jobqueue.JobBundle {
		return &jobqueue.JobBundle{
			ID:      bundleID,
			Type:    "bundle-type",
			Origin:  "origin",
			NumJobs: 1,
			Jobs:    []*jobqueue.Job{{ID: uu.IDFrom("c2a7c5b4-4b0c-4b3e-9a5a-1f2e3d4c5b6f"), Type: jobType, Origin: "origin", Payload: notnull.JSON(`{}`)}},
		}
	}

	t.Run("ignore callback", func(t *testing.T) {
		var service jobworkerDB
		var seen uu.NullableID
		ctx := ContextWithIgnoreJobBundle(t.Context(), func(b *jobqueue.JobBundle) bool {
			seen = b.Jobs[0].BundleID
			return true
		})
		require.NoError(t, service.AddJobBundle(ctx, newBundle("job-type")))
		assert.Equal(t, bundleID.Nullable(), seen)
	})

	t.Run("synchronous worker", func(t *testing.T) {
		const jobType = "test-bundle-id-sync-type"
		var seen uu.NullableID
		jobworker.Register(jobType, func(_ context.Context, job *jobqueue.Job) (any, error) {
			seen = job.BundleID
			return nil, nil
		})
		t.Cleanup(func() { jobworker.Unregister(jobType) })

		var service jobworkerDB
		require.NoError(t, service.AddJobBundle(ContextWithSynchronousJobs(t.Context()), newBundle(jobType)))
		assert.Equal(t, bundleID.Nullable(), seen)
	})
}
