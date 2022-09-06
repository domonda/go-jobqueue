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
