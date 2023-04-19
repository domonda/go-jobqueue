package jobworkerdb

import (
	"context"

	"github.com/domonda/go-jobqueue"
	"github.com/domonda/go-jobqueue/jobworker"
	rootlog "github.com/domonda/golog/log"
)

var log = rootlog.NewPackageLogger("jobworkerdb")

func InitJobQueue(ctx context.Context) error {
	db := new(jobworkerDB)

	err := db.AddListener(ctx, jobqueue.NewDefaultServiceListener(db))
	if err != nil {
		return err
	}

	jobworker.SetDataBase(db)

	jobqueue.SetDefaultService(db)
	return nil
}
