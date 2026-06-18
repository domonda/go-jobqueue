package jobqueue_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/domonda/go-types/nullable"
	"github.com/domonda/go-types/uu"

	"github.com/domonda/go-jobqueue"
)

func TestNewJobBundle(t *testing.T) {
	ctx := t.Context()

	t.Run("empty descriptions returns error", func(t *testing.T) {
		bundle, err := jobqueue.NewJobBundle(ctx, "bundleType", "origin", nil, nullable.Time{})
		require.Error(t, err)
		assert.Nil(t, bundle)
	})

	t.Run("creates one job per description", func(t *testing.T) {
		descs := []jobqueue.JobDesc{
			{Type: "type-a", Payload: "{}", Origin: "origin-a", Priority: 5},
			{Type: "type-b", Payload: "{}", Origin: "origin-b"},
		}
		bundle, err := jobqueue.NewJobBundle(ctx, "bundleType", "bundleOrigin", descs, nullable.Time{})
		require.NoError(t, err)

		assert.Equal(t, "bundleType", bundle.Type)
		assert.Equal(t, "bundleOrigin", bundle.Origin)
		assert.Equal(t, 2, bundle.NumJobs)
		assert.NotEqual(t, uu.IDNil, bundle.ID, "bundle gets an ID")
		require.Len(t, bundle.Jobs, 2)

		assert.Equal(t, "type-a", bundle.Jobs[0].Type)
		assert.Equal(t, "origin-a", bundle.Jobs[0].Origin)
		assert.Equal(t, int64(5), bundle.Jobs[0].Priority)
		assert.Equal(t, "type-b", bundle.Jobs[1].Type)
		assert.NotEqual(t, bundle.Jobs[0].ID, bundle.Jobs[1].ID, "each job gets a unique ID")
	})

	t.Run("empty desc type falls back to reflection", func(t *testing.T) {
		payload := time.Time{}
		descs := []jobqueue.JobDesc{{Payload: payload, Origin: "origin"}}
		bundle, err := jobqueue.NewJobBundle(ctx, "bundleType", "bundleOrigin", descs, nullable.Time{})
		require.NoError(t, err)
		assert.Equal(t, jobqueue.ReflectJobTypeOfPayload(payload), bundle.Jobs[0].Type)
	})

	t.Run("propagates job creation error", func(t *testing.T) {
		// A nil payload makes NewJobWithPriority fail, which must abort the bundle.
		descs := []jobqueue.JobDesc{{Type: "type-a", Payload: nil, Origin: "origin"}}
		bundle, err := jobqueue.NewJobBundle(ctx, "bundleType", "bundleOrigin", descs, nullable.Time{})
		require.Error(t, err)
		assert.Nil(t, bundle)
	})
}

func TestJobBundleHasError(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var nilBundle *jobqueue.JobBundle
		assert.False(t, nilBundle.HasError())
	})

	t.Run("no jobs", func(t *testing.T) {
		assert.False(t, (&jobqueue.JobBundle{}).HasError())
	})

	t.Run("no job has an error", func(t *testing.T) {
		bundle := &jobqueue.JobBundle{Jobs: []*jobqueue.Job{{}, {}}}
		assert.False(t, bundle.HasError())
	})

	t.Run("a job has an error", func(t *testing.T) {
		errored := &jobqueue.Job{}
		errored.ErrorMsg.Set("boom")
		bundle := &jobqueue.JobBundle{Jobs: []*jobqueue.Job{{}, errored}}
		assert.True(t, bundle.HasError())
	})
}

func TestJobBundleString(t *testing.T) {
	var nilBundle *jobqueue.JobBundle
	assert.Equal(t, "nil JobBundle", nilBundle.String(), "nil receiver has a fixed string")

	bundle := &jobqueue.JobBundle{Type: "bundleType", Origin: "bundleOrigin"}
	str := bundle.String()
	assert.Contains(t, str, "bundleType")
	assert.Contains(t, str, "bundleOrigin")
}
