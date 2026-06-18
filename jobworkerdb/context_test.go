package jobworkerdb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworkerdb"
)

func TestSynchronousJobs(t *testing.T) {
	ctx := t.Context()
	assert.False(t, jobworkerdb.SynchronousJobs(ctx), "plain context is not synchronous")
	assert.True(t, jobworkerdb.SynchronousJobs(jobworkerdb.ContextWithSynchronousJobs(ctx)))
}

func TestIgnoreJob(t *testing.T) {
	ctx := t.Context()
	job := &jobqueue.Job{Type: "type-a"}

	t.Run("no filter set returns false", func(t *testing.T) {
		assert.False(t, jobworkerdb.IgnoreJob(ctx, job))
	})

	t.Run("ContextWithIgnoreJob applies the filter", func(t *testing.T) {
		ignoreCtx := jobworkerdb.ContextWithIgnoreJob(ctx, jobworkerdb.IgnoreAllJobs)
		assert.True(t, jobworkerdb.IgnoreJob(ignoreCtx, job))

		keepCtx := jobworkerdb.ContextWithIgnoreJob(ctx, func(*jobqueue.Job) bool { return false })
		assert.False(t, jobworkerdb.IgnoreJob(keepCtx, job))
	})

	t.Run("ContextWithIgnoreJobType matches only the named type", func(t *testing.T) {
		ignoreCtx := jobworkerdb.ContextWithIgnoreJobType(ctx, "type-a")
		assert.True(t, jobworkerdb.IgnoreJob(ignoreCtx, &jobqueue.Job{Type: "type-a"}))
		assert.False(t, jobworkerdb.IgnoreJob(ignoreCtx, &jobqueue.Job{Type: "type-b"}))
	})
}

func TestIgnoreJobBundle(t *testing.T) {
	ctx := t.Context()
	bundle := &jobqueue.JobBundle{Type: "bundle-a"}

	t.Run("no filter set returns false", func(t *testing.T) {
		assert.False(t, jobworkerdb.IgnoreJobBundle(ctx, bundle))
	})

	t.Run("ContextWithIgnoreJobBundle applies the filter", func(t *testing.T) {
		ignoreCtx := jobworkerdb.ContextWithIgnoreJobBundle(ctx, jobworkerdb.IgnoreAllJobBundles)
		assert.True(t, jobworkerdb.IgnoreJobBundle(ignoreCtx, bundle))

		keepCtx := jobworkerdb.ContextWithIgnoreJobBundle(ctx, func(*jobqueue.JobBundle) bool { return false })
		assert.False(t, jobworkerdb.IgnoreJobBundle(keepCtx, bundle))
	})
}

func TestIgnoreAllHelpers(t *testing.T) {
	assert.True(t, jobworkerdb.IgnoreAllJobs(nil))
	assert.True(t, jobworkerdb.IgnoreAllJobBundles(nil))
}
