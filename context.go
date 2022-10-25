package jobqueue

import (
	"context"
)

var synchronousJobsKey int

func ContextWithSynchronousJobs(ctx context.Context) context.Context {
	return context.WithValue(ctx, &synchronousJobsKey, struct{}{})
}

func SynchronousJobs(ctx context.Context) bool {
	return ctx.Value(&synchronousJobsKey) != nil
}

var ignoreJobKey int

type IgnoreJobFunc func(*Job) bool

func IgnoreAllJobs(*Job) bool { return true }

func ContextWithIgnoreJob(ctx context.Context, ignoreJob IgnoreJobFunc) context.Context {
	return context.WithValue(ctx, &ignoreJobKey, ignoreJob)
}

func IgnoreJob(ctx context.Context, job *Job) bool {
	if ignoreJob, ok := ctx.Value(&ignoreJobKey).(IgnoreJobFunc); ok {
		return ignoreJob(job)
	}
	return false
}

var ignoreJobBundleKey int

type IgnoreJobBundleFunc func(*JobBundle) bool

func IgnoreAllJobBundles(*JobBundle) bool { return true }

func ContextWithIgnoreJobBundle(ctx context.Context, ignoreJobBundle IgnoreJobBundleFunc) context.Context {
	return context.WithValue(ctx, &ignoreJobBundleKey, ignoreJobBundle)
}

func IgnoreJobBundle(ctx context.Context, jobBundle *JobBundle) bool {
	if ignoreJobBundle, ok := ctx.Value(&ignoreJobBundleKey).(IgnoreJobBundleFunc); ok {
		return ignoreJobBundle(jobBundle)
	}
	return false
}
