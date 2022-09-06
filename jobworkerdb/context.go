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

var shouldDoJobKey int

type ShouldDoJobFunc func(*jobqueue.Job) bool

func NeverDoJobs(*jobqueue.Job) bool { return false }

func ContextWithShouldDoJob(ctx context.Context, shouldDoJob ShouldDoJobFunc) context.Context {
	return context.WithValue(ctx, &shouldDoJobKey, shouldDoJob)
}

func ShouldDoJob(ctx context.Context, job *jobqueue.Job) bool {
	if shouldDoJob, ok := ctx.Value(&shouldDoJobKey).(ShouldDoJobFunc); ok {
		return shouldDoJob(job)
	}
	return true
}

var shouldDoJobBundleKey int

type ShouldDoJobBundleFunc func(*jobqueue.JobBundle) bool

func NeverDoJobBundles(*jobqueue.JobBundle) bool { return false }

func ContextWithShouldDoJobBundle(ctx context.Context, shouldDoJobBundle ShouldDoJobBundleFunc) context.Context {
	return context.WithValue(ctx, &shouldDoJobBundleKey, shouldDoJobBundle)
}

func ShouldDoJobBundle(ctx context.Context, jobBundle *jobqueue.JobBundle) bool {
	if shouldDoJobBundle, ok := ctx.Value(&shouldDoJobBundleKey).(ShouldDoJobBundleFunc); ok {
		return shouldDoJobBundle(jobBundle)
	}
	return true
}
