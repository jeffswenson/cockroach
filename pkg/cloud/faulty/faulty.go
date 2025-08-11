package faulty

import (
	"context"
	"io"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func WrapStorage(
	wrappedStorage cloud.ExternalStorage, strategy fault.Strategy,
) cloud.ExternalStorage {
	if strategy == nil || wrappedStorage == nil {
		return wrappedStorage
	}
	return &faultyStorage{
		wrappedStorage: wrappedStorage,
		strategy:       strategy,
	}
}

// TODO(jeffswenson): we should adjust the op implementations so that we
// sometimes return an error for operations that succeed.
type faultyStorage struct {
	wrappedStorage cloud.ExternalStorage
	strategy       fault.Strategy
}

// Close implements cloud.ExternalStorage.
func (f *faultyStorage) Close() error {
	return f.wrappedStorage.Close()
}

// Conf implements cloud.ExternalStorage.
func (f *faultyStorage) Conf() cloudpb.ExternalStorage {
	return f.wrappedStorage.Conf()
}

func (f *faultyStorage) injectErr(ctx context.Context, opName string, basename string) error {
	if !f.strategy.ShouldInject(ctx, opName) {
		return nil
	}
	log.Infof(ctx, "injected error for %s %s: %s", opName, basename, debug.Stack())
	return errors.Newf("%s failed: injected error for '%s'", opName, basename)
}

// Delete implements cloud.ExternalStorage.
func (f *faultyStorage) Delete(ctx context.Context, basename string) error {
	if err := f.injectErr(ctx, "externalstorage.delete", basename); err != nil {
		return err
	}
	return f.wrappedStorage.Delete(ctx, basename)
}

// ExternalIOConf implements cloud.ExternalStorage.
func (f *faultyStorage) ExternalIOConf() base.ExternalIODirConfig {
	return f.wrappedStorage.ExternalIOConf()
}

// List implements cloud.ExternalStorage.
func (f *faultyStorage) List(
	ctx context.Context, prefix string, delimiter string, fn cloud.ListingFn,
) error {
	if err := f.injectErr(ctx, "externalstorage.list", prefix); err != nil {
		return err
	}
	return f.wrappedStorage.List(ctx, prefix, delimiter, fn)
}

// ReadFile implements cloud.ExternalStorage.
func (f *faultyStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (_ ioctx.ReadCloserCtx, fileSize int64, _ error) {
	log.Infof(ctx, "read %s", basename)
	// TODO(jeffswenson): we should also wrap the ReadCloserCtx so that we
	// sometimes return an error after opening successfully.
	if err := f.injectErr(ctx, "externalstorage.read", basename); err != nil {
		return nil, 0, err
	}
	return f.wrappedStorage.ReadFile(ctx, basename, opts)
}

// RequiresExternalIOAccounting implements cloud.ExternalStorage.
func (f *faultyStorage) RequiresExternalIOAccounting() bool {
	return f.wrappedStorage.RequiresExternalIOAccounting()
}

// Settings implements cloud.ExternalStorage.
func (f *faultyStorage) Settings() *cluster.Settings {
	return f.wrappedStorage.Settings()
}

// Size implements cloud.ExternalStorage.
func (f *faultyStorage) Size(ctx context.Context, basename string) (int64, error) {
	if err := f.injectErr(ctx, "externalstorage.size", basename); err != nil {
		return 0, err
	}
	return f.wrappedStorage.Size(ctx, basename)
}

// Writer implements cloud.ExternalStorage.
func (f *faultyStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	log.Infof(ctx, "write %s", basename)
	if err := f.injectErr(ctx, "externalstorage.writer", basename); err != nil {
		return nil, err
	}
	writer, err := f.wrappedStorage.Writer(ctx, basename)
	if err != nil {
		return nil, err
	}
	return &faultyWriter{
		wrappedWriter: writer,
		strategy:      f.strategy,
		basename:      basename,
	}, nil
}

var _ cloud.ExternalStorage = &faultyStorage{}
