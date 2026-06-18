package jobqueue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/domonda/go-jobqueue"
)

func TestStatusIsZero(t *testing.T) {
	var nilStatus *jobqueue.Status
	assert.True(t, nilStatus.IsZero(), "nil receiver is zero")
	assert.True(t, (&jobqueue.Status{}).IsZero(), "zero value is zero")
	assert.False(t, (&jobqueue.Status{NumJobs: 1}).IsZero())
	assert.False(t, (&jobqueue.Status{NumJobBundles: 1}).IsZero())
}

func TestStatusString(t *testing.T) {
	var nilStatus *jobqueue.Status
	assert.Equal(t, "nil Status", nilStatus.String(), "nil receiver has a fixed string")
	assert.Equal(t,
		"Status{NumJobs: 2, NumJobBundles: 3}",
		(&jobqueue.Status{NumJobs: 2, NumJobBundles: 3}).String(),
	)
}
