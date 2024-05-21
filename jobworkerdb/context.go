package jobworkerdb

import (
	"context"

	"github.com/domonda/go-jobqueue"
)

var synchronousJobsKey int

func ContextWithSynchronousJobs(ctx context.Context) context.Context {
	return context.WithValue(ctx, &synchronousJobsKey, struct{}{})
}

func SynchronousJobs(ctx context.Context) bool {
	return ctx.Value(&synchronousJobsKey) != nil
}

var ignoreJobKey int

type IgnoreJobFunc func(*jobqueue.Job) bool

func IgnoreAllJobs(*jobqueue.Job) bool { return true }

func ContextWithIgnoreJob(ctx context.Context, ignoreJob IgnoreJobFunc) context.Context {
	return context.WithValue(ctx, &ignoreJobKey, ignoreJob)
}

func ContextWithIgnoreJobType(ctx context.Context, ignoreJobType string) context.Context {
	return ContextWithIgnoreJob(ctx, func(job *jobqueue.Job) (ignore bool) {
		return job.Type == ignoreJobType
	})
}

func IgnoreJob(ctx context.Context, job *jobqueue.Job) bool {
	if ignoreJob, ok := ctx.Value(&ignoreJobKey).(IgnoreJobFunc); ok {
		return ignoreJob(job)
	}
	return false
}

var ignoreJobBundleKey int

type IgnoreJobBundleFunc func(*jobqueue.JobBundle) bool

func IgnoreAllJobBundles(*jobqueue.JobBundle) bool { return true }

func ContextWithIgnoreJobBundle(ctx context.Context, ignoreJobBundle IgnoreJobBundleFunc) context.Context {
	return context.WithValue(ctx, &ignoreJobBundleKey, ignoreJobBundle)
}

func IgnoreJobBundle(ctx context.Context, jobBundle *jobqueue.JobBundle) bool {
	if ignoreJobBundle, ok := ctx.Value(&ignoreJobBundleKey).(IgnoreJobBundleFunc); ok {
		return ignoreJobBundle(jobBundle)
	}
	return false
}
