package sstmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

type ingestJob struct {
}

// CollectProfile implements jobs.Resumer.
func (i *ingestJob) CollectProfile(ctx context.Context, execCtx interface{}) error {
	// TODO(jeffswenson): why is this an option on jobs.Resumer?
	return nil
}

// OnFailOrCancel implements jobs.Resumer.
func (i *ingestJob) OnFailOrCancel(ctx context.Context, execCtx interface{}, jobErr error) error {
	panic("unimplemented")
}

// Resume implements jobs.Resumer.
func (i *ingestJob) Resume(ctx context.Context, execCtx interface{}) error {
	// TODO(jeffswenson): the plan for graceful resume is each processor will
	// periodically (and on drain) checkpoint their progress in a per-processor
	// job_info key. When the job resumes, it will list all of the info keys, and
	// use that to recover the state of the job.
	if err := i.merge(ctx); err != nil {
		return err
	}

	if err := i.splitAndScatter(ctx); err != nil {
		return err
	}

	return i.addSsts(ctx)
}

func (i *ingestJob) merge(ctx context.Context) error {
	// NOTE: logicalReplicationResumer.ingest is a good example for how this
	// should work.

	// TODO(jeffswenson): create a task set with a task for each split
	// TODO(jeffswenson): create a query plan that places a worker on each node
	// TODO(jeffswenson): assign a initial task to each worker and start the worker
	// TODO(jeffswenson): wait for all workers to finish

	// panic("unimplemented")
	return nil
}

func (i *ingestJob) splitAndScatter(ctx context.Context) error {
	return nil
}

func (i *ingestJob) addSsts(ctx context.Context) error {
	// TODO(jeffswenson): create a task set with a task for each merged worker
	// TODO(jeffswenson): create a query plan that places a worker on each node
	// TODO(jeffswenson): assign a initial task to each worker and start the worker
	// TODO(jeffswenson): wait for all workers to finish

	// panic("unimplemented")
	return errors.New("unimplemented")
}

var _ jobs.Resumer = (*ingestJob)(nil)

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeSSTIngest,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &ingestJob{}
		},
		jobs.UsesTenantCostControl,
	)
}
