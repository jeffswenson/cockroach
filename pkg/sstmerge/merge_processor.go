package sstmerge

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

type mergeProcessor struct {
	// options
	flushSize int

	// inputs
	coordinator *mergeCoordinator
	spans       []Span
	ssts        []SstMetadata

	// outputs
	cache *pebble.Cache

	// TODO(jeffswenson): in theory, the input and output could be different
	// object stores
	outputPrefix string
	store        ObjectStore
}

func (mp *mergeProcessor) run(ctx context.Context) ([]MergedSpan, error) {
	var mergedSpans []MergedSpan
	for task, ok := mp.coordinator.ClaimFirst(); ok; task, ok = mp.coordinator.ClaimNext(task) {
		mergedSpan, err := mp.process(ctx, task)
		if err != nil {
			return nil, err
		}
		mergedSpans = append(mergedSpans, mergedSpan)
	}
	return mergedSpans, nil
}

func (mp *mergeProcessor) process(ctx context.Context, task taskId) (MergedSpan, error) {
	span := mp.spans[int(task)]

	// find overlapping ssts
	var files [][]sstable.ReadableFile
	for _, sst := range mp.ssts {
		if sst.Span.Overlaps(span) {
			file, err := mp.store.Read(ctx, sst.Filename)
			if err != nil {
				return MergedSpan{}, err
			}
			files = append(files, []sstable.ReadableFile{file})
		}
	}

	options := storage.DefaultPebbleOptions()
	merged, err := pebble.NewExternalIter(
		options,
		&pebble.IterOptions{
			LowerBound: span.Start,
			UpperBound: span.End,
		}, files)
	if err != nil {
		return MergedSpan{}, err
	}
	defer func() {
		_ = merged.Close()
	}()

	// TODO: add

	for i := 0; merged.Valid(); i++ {
		outputName := fmt.Sprintf("%s/task-%d-flush-%d.sst", mp.outputPrefix, task, 0)

		out, err := mp.store.Write(ctx, outputName)
		if err != nil {
			return MergedSpan{}, err
		}

		sstWriter := sstable.NewWriter(out, options.MakeWriterOptions(0, sstable.TableFormatPebblev5))
		defer func() {
			closeErr := sstWriter.Close()
			if err == nil {
				err = closeErr
			}
		}()

		// TODO(jeffswenson): use storage.SSTWriter
		// TODO(jeffswenson): flush large SSTs

		merged.First()
		for merged.Valid() {

			if err := sstWriter.Set(merged.Key(), merged.Value()); err != nil {
				return MergedSpan{}, err
			}
			merged.Next()
		}
	}

	return MergedSpan{}, nil
}
