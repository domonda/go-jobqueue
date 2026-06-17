package jobworkerdb

import (
	"context"

	"github.com/domonda/go-jobqueue"
)

var synchronousJobsKey int

// ContextWithSynchronousJobs returns a context that makes added jobs run inline
// (synchronously) instead of being persisted to the database for a worker to
// pick up. Use SynchronousJobs to test a context for this flag.
func ContextWithSynchronousJobs(ctx context.Context) context.Context {
	return context.WithValue(ctx, &synchronousJobsKey, struct{}{})
}

// SynchronousJobs reports whether ctx was marked for synchronous job execution
// by ContextWithSynchronousJobs.
func SynchronousJobs(ctx context.Context) bool {
	return ctx.Value(&synchronousJobsKey) != nil
}

var ignoreJobKey int

// IgnoreJobFunc reports whether the given job should be ignored,
// meaning silently discarded instead of added to the queue.
type IgnoreJobFunc func(*jobqueue.Job) bool

// IgnoreAllJobs is an IgnoreJobFunc that ignores every job.
func IgnoreAllJobs(*jobqueue.Job) bool { return true }

// ContextWithIgnoreJob returns a context whose added jobs are silently discarded
// when ignoreJob returns true. The filter is applied by IgnoreJob.
func ContextWithIgnoreJob(ctx context.Context, ignoreJob IgnoreJobFunc) context.Context {
	return context.WithValue(ctx, &ignoreJobKey, ignoreJob)
}

// ContextWithIgnoreJobType returns a context that silently discards added jobs
// whose Type equals ignoreJobType.
func ContextWithIgnoreJobType(ctx context.Context, ignoreJobType string) context.Context {
	return ContextWithIgnoreJob(ctx, func(job *jobqueue.Job) (ignore bool) {
		return job.Type == ignoreJobType
	})
}

// IgnoreJob reports whether job should be ignored according to the IgnoreJobFunc
// stored in ctx by ContextWithIgnoreJob. It returns false if no filter is set.
func IgnoreJob(ctx context.Context, job *jobqueue.Job) bool {
	if ignoreJob, ok := ctx.Value(&ignoreJobKey).(IgnoreJobFunc); ok {
		return ignoreJob(job)
	}
	return false
}

var ignoreJobBundleKey int

// IgnoreJobBundleFunc reports whether the given job bundle should be ignored,
// meaning silently discarded instead of added to the queue.
type IgnoreJobBundleFunc func(*jobqueue.JobBundle) bool

// IgnoreAllJobBundles is an IgnoreJobBundleFunc that ignores every job bundle.
func IgnoreAllJobBundles(*jobqueue.JobBundle) bool { return true }

// ContextWithIgnoreJobBundle returns a context whose added job bundles are
// silently discarded when ignoreJobBundle returns true. The filter is applied
// by IgnoreJobBundle.
func ContextWithIgnoreJobBundle(ctx context.Context, ignoreJobBundle IgnoreJobBundleFunc) context.Context {
	return context.WithValue(ctx, &ignoreJobBundleKey, ignoreJobBundle)
}

// IgnoreJobBundle reports whether jobBundle should be ignored according to the
// IgnoreJobBundleFunc stored in ctx by ContextWithIgnoreJobBundle. It returns
// false if no filter is set.
func IgnoreJobBundle(ctx context.Context, jobBundle *jobqueue.JobBundle) bool {
	if ignoreJobBundle, ok := ctx.Value(&ignoreJobBundleKey).(IgnoreJobBundleFunc); ok {
		return ignoreJobBundle(jobBundle)
	}
	return false
}
