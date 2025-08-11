package faulty

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/errors"
)

type faultyWriter struct {
	wrappedWriter io.WriteCloser
	strategy      fault.Strategy
	basename      string
}

func (f *faultyWriter) maybeInjectErr(ctx context.Context, opName string) error {
	if !f.strategy.ShouldInject(ctx, opName) {
		return nil
	}
	return errors.Newf("injected error for op '%s' on object '%s'", opName, f.basename)
}

func (f *faultyWriter) Close() error {
	err := f.wrappedWriter.Close()
	if err != nil {
		return err
	}
	return f.maybeInjectErr(context.Background(), "faultyWriter.Close")
}

func (f *faultyWriter) Write(p []byte) (n int, err error) {
	if err := f.maybeInjectErr(context.Background(), "faultyWriter.Write"); err != nil {
		return 0, err
	}
	return f.wrappedWriter.Write(p)
}
