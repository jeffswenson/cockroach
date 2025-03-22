package bulkmerge

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type flusher interface {
	// PutRawMVCCValue writes a raw MVCC key-value pair.
	PutRawMVCCValue(ctx context.Context, key storage.MVCCKey, value []byte) error
	// Flush finalizes the current SST and returns the list of generated SSTs.
	Flush(ctx context.Context) (bulksst.SSTFiles, error)
}

// ingestFlusher implements the flusher interface by wrapping a bulk.SSTBatcher.
// It flushes data directly to the storage layer rather than returning SST files.
type ingestFlusher struct {
	batcher *bulk.SSTBatcher
	flowCtx *execinfra.FlowCtx
}

// newIngestFlusher creates a new ingestFlusher that writes directly to storage.
func newIngestFlusher(ctx context.Context, flowCtx *execinfra.FlowCtx) (*ingestFlusher, error) {
	return &ingestFlusher{flowCtx: flowCtx}, nil
}

func (f *ingestFlusher) createBatcher(ctx context.Context) error {
	if f.batcher != nil {
		return nil
	}
	batcher, err := bulk.MakeSSTBatcher(
		ctx,
		"bulk-merge",
		f.flowCtx.Cfg.DB.KV(),
		f.flowCtx.Cfg.Settings,
		hlc.Timestamp{},
		false,
		true, // Scatter ranges
		f.flowCtx.Mon.MakeConcurrentBoundAccount(),
		f.flowCtx.Cfg.BulkSenderLimiter,
	)
	if err != nil {
		return err
	}
	f.batcher = batcher
	return nil
}

// PutRawMVCCValue implements the flusher interface.
func (f *ingestFlusher) PutRawMVCCValue(
	ctx context.Context, key storage.MVCCKey, value []byte,
) error {
	if err := f.createBatcher(ctx); err != nil {
		return err
	}
	return f.batcher.AddMVCCKey(ctx, key, value)
}

// Flush implements the flusher interface.
func (f *ingestFlusher) Flush(ctx context.Context) (bulksst.SSTFiles, error) {
	if f.batcher == nil {
		return bulksst.SSTFiles{}, nil
	}
	if err := f.batcher.Flush(ctx); err != nil {
		return bulksst.SSTFiles{}, err
	}
	f.batcher = nil
	// Return empty SSTFiles since data is flushed directly to storage
	return bulksst.SSTFiles{}, nil
}
