package jobworkerdb

import (
	"context"
)

var synchronousJobWorkerKey int

func ContextWithSynchronousJobWorker(ctx context.Context) context.Context {
	return context.WithValue(ctx, &synchronousJobWorkerKey, struct{}{})
}

func SynchronousJobWorker(ctx context.Context) bool {
	return ctx.Value(&synchronousJobWorkerKey) != nil
}

var discardingJobWorkerKey int

func ContextWithDiscardingJobWorker(ctx context.Context) context.Context {
	return context.WithValue(ctx, &discardingJobWorkerKey, struct{}{})
}

func DiscardingJobWorker(ctx context.Context) bool {
	return ctx.Value(&discardingJobWorkerKey) != nil
}
