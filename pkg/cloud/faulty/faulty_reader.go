package faulty

import (
	"context"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
)

type faultyReader struct {
	wrappedReader ioctx.ReadCloserCtx
	strategy      fault.Strategy
	basename      string
}

func (f *faultyReader) maybeInjectErr(ctx context.Context, opName string) error {
	if !f.strategy.ShouldInject(ctx, opName) {
		return nil
	}
	return errors.Newf("injected error for op '%s' on object '%s':\n %s", opName, f.basename, debug.Stack())
}

// Close implements ioctx.ReadCloserCtx.
func (f *faultyReader) Close(context.Context) error {
	err := f.wrappedReader.Close(context.Background())
	if err != nil {
		return err
	}
	return f.maybeInjectErr(context.Background(), "faultyReader.Close")
}

// Read implements ioctx.ReadCloserCtx.
func (f *faultyReader) Read(ctx context.Context, p []byte) (n int, err error) {
	if err := f.maybeInjectErr(ctx, "faultyReader.Read"); err != nil {
		return 0, err
	}
	return f.wrappedReader.Read(ctx, p)
}

var _ ioctx.ReadCloserCtx = &faultyReader{}
