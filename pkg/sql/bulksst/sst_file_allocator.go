// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/vfs"
)

var MaxRowSampleBytes = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"bulkio.max_row_sample_bytes",
	"maximum size in bytes of row samples to keep for bulk ingestion",
	10*1024*1024, // 10MB
)

// FileAllocator is used to allocate new files for SSTs ingested via the Writer.
type FileAllocator interface {
	// AddFile creates a new file and stores the URI for tracking.
	AddFile(
		ctx context.Context, fileIndex int, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
	) (objstorage.Writable, func(), error)

	// GetFileList gets all the files created by this file allocator.
	GetFileList() *SSTFiles
}

// fileAllocatorBase helps track metadata for created SST files.
type fileAllocatorBase struct {
	fileInfo       SSTFiles
	rowSampleBytes int64
	rand           *rand.Rand
	settings       *cluster.Settings
}

// GetFileList gets all the files created by this file allocator.
func (f *fileAllocatorBase) GetFileList() *SSTFiles {
	return &f.fileInfo
}

func NewFileAllocatorBase(st *cluster.Settings) fileAllocatorBase {
	return fileAllocatorBase{
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		settings: st,
	}
}

// addFile helps track metadata for created SST files.
func (f *fileAllocatorBase) addFile(
	uri string, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) {
	f.fileInfo.SST = append(f.fileInfo.SST, &SSTFileInfo{
		URI:      uri,
		StartKey: string(span.Key),
		EndKey:   string(span.EndKey),
		FileSize: fileSize,
	})
	f.fileInfo.TotalSize += fileSize
	f.fileInfo.RowSamples = append(f.fileInfo.RowSamples, string(rowSample))
	f.rowSampleBytes += int64(len(rowSample))
	f.maybeResampleRowSample()
}

// maybeResampleRowSample resamples the row sample if it becomes too large.
func (f *fileAllocatorBase) maybeResampleRowSample() {
	// If we haven't exceeded the limit, don't do anything.
	if f.rowSampleBytes < MaxRowSampleBytes.Get(&f.settings.SV) {
		return
	}
	// Shuffle the samples to randomly pick ones to discard.
	f.rand.Shuffle(len(f.fileInfo.RowSamples), func(i, j int) {
		f.fileInfo.RowSamples[i], f.fileInfo.RowSamples[j] = f.fileInfo.RowSamples[j], f.fileInfo.RowSamples[i]
	})
	// Disacrd 1 / 4th of the samples.
	samplesToKeep := max((len(f.fileInfo.RowSamples)*3)/4, 1)
	newRowSamples := f.fileInfo.RowSamples[:samplesToKeep]
	for i := samplesToKeep; i < len(f.fileInfo.RowSamples); i++ {
		f.rowSampleBytes -= int64(len(f.fileInfo.RowSamples[i]))
	}
	f.fileInfo.RowSamples = newRowSamples
}

// VFSFileAllocator allocates local files for storing SSTs.
type VFSFileAllocator struct {
	fileAllocatorBase
	baseName string
	storage  vfs.FS
}

// NewVFSFileAllocator creates a new file allocator with baseName and a VFS.
func NewVFSFileAllocator(baseName string, storage vfs.FS, st *cluster.Settings) FileAllocator {
	return &VFSFileAllocator{
		baseName:          baseName,
		storage:           storage,
		fileAllocatorBase: NewFileAllocatorBase(st),
	}
}

// AddFile creates a new file and stores the URI for tracking.
func (f *VFSFileAllocator) AddFile(
	ctx context.Context, fileIndex int, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) (objstorage.Writable, func(), error) {
	fileName := fmt.Sprintf("%s_%d", f.baseName, fileIndex)
	writer, err := f.storage.Create(fileName, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, nil, err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	f.fileAllocatorBase.addFile(fileName, span, rowSample, fileSize)
	return remoteWritable, func() { writer.Close() }, nil
}

// ExternalFileAllocator allocates external files for SSTs.
type ExternalFileAllocator struct {
	es      cloud.ExternalStorage
	baseURI string
	fileAllocatorBase
}

func NewExternalFileAllocator(
	es cloud.ExternalStorage, baseURI string, st *cluster.Settings,
) FileAllocator {
	return &ExternalFileAllocator{
		es:                es,
		baseURI:           baseURI,
		fileAllocatorBase: NewFileAllocatorBase(st),
	}
}

// AddFile creates a new file and stores the URI for tracking.
func (e *ExternalFileAllocator) AddFile(
	ctx context.Context, fileIndex int, span roachpb.Span, rowSample roachpb.Key, fileSize uint64,
) (objstorage.Writable, func(), error) {
	fileName := fmt.Sprintf("%d.sst", fileIndex)
	writer, err := e.es.Writer(ctx, fileName)
	if err != nil {
		return nil, nil, err
	}
	remoteWritable := objstorageprovider.NewRemoteWritable(writer)
	e.fileAllocatorBase.addFile(e.baseURI+fileName, span, rowSample, fileSize)
	return remoteWritable, func() { writer.Close() }, nil
}
