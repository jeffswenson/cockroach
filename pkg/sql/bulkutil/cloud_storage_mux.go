// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil

import (
	"context"
	"io/fs"
	"net/url"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

// CloudStorageMux is a utility for managing multiple cloud storage instances.
// The main motivator for this is each node has its own nodelocal://<node-id>
// instance.
type CloudStorageMux struct {
	factory        cloud.ExternalStorageFromURIFactory
	storeInstances map[url.URL]cloud.ExternalStorage
	user           username.SQLUsername
}

func NewCloudStorageMux(
	factory cloud.ExternalStorageFromURIFactory, user username.SQLUsername,
) *CloudStorageMux {
	return &CloudStorageMux{
		factory:        factory,
		storeInstances: make(map[url.URL]cloud.ExternalStorage),
		user:           user,
	}
}

func (c *CloudStorageMux) Close() error {
	var err error
	for _, store := range c.storeInstances {
		err = errors.CombineErrors(err, store.Close())
	}
	return err
}

func (c *CloudStorageMux) StoreFile(ctx context.Context, uri string) (storageccl.StoreFile, error) {
	prefix, filepath, err := c.splitURI(uri)
	if err != nil {
		return storageccl.StoreFile{}, err
	}
	store, err := c.getStore(prefix)
	if err != nil {
		return storageccl.StoreFile{}, err
	}
	return storageccl.StoreFile{
		Store:    store,
		FilePath: filepath,
	}, nil
}

func (c *CloudStorageMux) Open(ctx context.Context, uri string) (sstable.ReadableFile, error) {
	url, path, err := c.splitURI(uri)
	if err != nil {
		return nil, err
	}

	store, err := c.getStore(url)
	if err != nil {
		return nil, err
	}

	size, err := store.Size(ctx, path)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get size of %s", path)
	}

	return &cloudSST{
		ctx:      ctx,
		store:    store,
		basename: path,
		size:     size,
	}, nil
}

func (c *CloudStorageMux) getStore(uri url.URL) (cloud.ExternalStorage, error) {
	store, ok := c.storeInstances[uri]
	if !ok {
		storage, err := c.factory(context.Background(), uri.String(), c.user)
		if err != nil {
			return nil, err
		}
		c.storeInstances[uri] = storage
		store = storage
	}
	return store, nil
}

func (c *CloudStorageMux) splitURI(uri string) (url.URL, string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return url.URL{}, "", errors.Wrap(err, "failed to parse external storage uri")
	}

	path := parsed.Path
	parsed.Path = ""

	return *parsed, path, nil
}

type cloudSST struct {
	ctx      context.Context
	store    cloud.ExternalStorage
	basename string
	size     int64

	stream   ioctx.ReadCloserCtx
	location int64
}

// DeviceID implements vfs.FileInfo.
func (c *cloudSST) DeviceID() vfs.DeviceID {
	panic("unimplemented")
}

// IsDir implements vfs.FileInfo.
func (c *cloudSST) IsDir() bool {
	panic("unimplemented")
}

// ModTime implements vfs.FileInfo.
func (c *cloudSST) ModTime() time.Time {
	if true {
		panic("is ModTime() needed?")
	}
	return time.Now()
}

// Mode implements vfs.FileInfo.
func (c *cloudSST) Mode() fs.FileMode {
	if true {
		panic("is Mode() needed")
	}
	return fs.FileMode(0)
}

// Name implements vfs.FileInfo.
func (c *cloudSST) Name() string {
	return c.basename
}

// Size implements vfs.FileInfo.
func (c *cloudSST) Size() int64 {
	return c.size
}

// Sys implements vfs.FileInfo.
func (c *cloudSST) Sys() any {
	if true {
		panic("is Sys() needed?")
	}
	return nil
}

func (c *cloudSST) ReadAt(p []byte, off int64) (n int, err error) {
	if c.stream != nil && c.location == off {
		// We are already at the correct location, just read from the stream.
		n, err = c.stream.Read(c.ctx, p)
		c.location += int64(n)
		return n, err
	}

	if c.stream != nil {
		c.stream.Close(c.ctx)
		c.stream = nil
		c.location = 0
	}

	// TODO(jeffswenson): should I cache this stream?
	stream, _, err := c.store.ReadFile(c.ctx, c.basename, cloud.ReadOptions{
		Offset: off,
		// TODO(jeffswenson): udpate nodelocal is it uses this hint
		LengthHint: int64(len(p)),
		NoFileSize: true,
	})
	if err != nil {
		return 0, err
	}
	defer stream.Close(c.ctx)

	n, err = stream.Read(c.ctx, p)
	if err != nil {
		return 0, err
	}

	c.location = off + int64(n)
	c.stream = stream

	return n, nil
}

func (c *cloudSST) Close() error {
	// no-op, resources are not owned by this object
	return nil
}

func (c *cloudSST) Stat() (vfs.FileInfo, error) {
	return c, nil
}

var _ sstable.ReadableFile = &cloudSST{}
var _ vfs.FileInfo = &cloudSST{}
