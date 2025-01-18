package sstmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

type ObjectStore interface {
	Read(ctx context.Context, name string) (vfs.File, error)
	Write(ctx context.Context, name string) (objstorage.Writable, error)
}

func NewMemoryObjectStore() ObjectStore {
	return &vfsObjectStore{vfs.NewMem()}
}

type vfsObjectStore struct {
	fs vfs.FS
}

func (v *vfsObjectStore) Read(ctx context.Context, name string) (vfs.File, error) {
	return v.fs.Open(name)
}

func (v *vfsObjectStore) Write(ctx context.Context, name string) (objstorage.Writable, error) {
	file, err := v.fs.Create(name, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	return objstorageprovider.NewFileWritable(file), nil
}

type Merger struct {
	cache *pebble.Cache
	store ObjectStore
}

// Okay: so what do I actually need to do here?
//  1. InputSSTs should include start and end keys for each SST.
//  2. Merging is supplied a start and end key to merge.
//  3. Merging can flush more than one SST. So it needs to pick the output
//     names.
func (f *Merger) Merge(
	ctx context.Context, outputName string, inputSSTs []string, span Span,
) (err error) {
	files := [][]sstable.ReadableFile{}
	for _, inputName := range inputSSTs {
		file, err := f.store.Read(ctx, inputName)
		if err != nil {
			// TODO clean up errors if this fails
			return err
		}
		files = append(files, []sstable.ReadableFile{file})
	}

	options := storage.DefaultPebbleOptions()

	merged, err := pebble.NewExternalIter(
		options,
		&pebble.IterOptions{
			LowerBound: span.Start,
			UpperBound: span.End,
		}, files)
	if err != nil {
		return err
	}
	defer func() {
		_ = merged.Close()
	}()

	out, err := f.store.Write(ctx, outputName)
	if err != nil {
		return err
	}

	sstWriter := sstable.NewWriter(out, options.MakeWriterOptions(0, sstable.TableFormatPebblev5))
	defer func() {
		closeErr := sstWriter.Close()
		if err == nil {
			err = closeErr
		}
	}()

	merged.First()
	for merged.Valid() {
		if err := sstWriter.Set(merged.Key(), merged.Value()); err != nil {
			return err
		}
		merged.Next()
	}

	return nil
}
