package queuefeed

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TODO this should probably be connected to the rangfeed size in some way.
const maxBufSize = 1000

func newTableFeed(ctx context.Context, feedName string, table descpb.ID, frontier span.Frontier, rff *rangefeed.Factory, leaseMgr *lease.Manager, codec *keys.SQLCodec) (*tableFeed, error) {
	tf := &tableFeed{
		codec:    *codec,
		tableID:  descpb.ID(table),
		leaseMgr: leaseMgr,
	}

	if frontier.Frontier().IsEmpty() {
		return nil, errors.AssertionFailedf("frontier is empty")
	}

	// setup rangefeed on data
	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("queuefeed.tablereader", feedName),
		// rangefeed.WithMemoryMonitor(w.mon),
		rangefeed.WithOnCheckpoint(tf.onRangfeedCheckpoint),
		rangefeed.WithOnInternalError(tf.setFailed),
		rangefeed.WithConsumerID(42),
		rangefeed.WithInvoker(func(fn func() error) error { return fn() }),
		rangefeed.WithFiltering(false),
	}

	tf.rangefeed = rff.New(
		fmt.Sprintf("queuefeed.reader.name=%s", feedName), frontier.Frontier(), tf.onRangfeedValue, opts...,
	)

	if err := tf.rangefeed.StartFromFrontier(ctx, frontier); err != nil {
		return nil, errors.Wrap(err, "starting rangefeed")
	}

	return tf, nil
}


// bufferedEvent represents either a data row or a checkpoint timestamp
// in the reader's buffer. Exactly one of row or resolved will be set.
type bufferedEvent struct {
	// row is set for data events. nil for checkpoint events.
	row tree.Datums
	// resolved is set for checkpoint events. Empty for data events.
	resolved hlc.Timestamp
}

// tableFeed is a rangfeed wrapper that decodes rows into a local buffer that
// clients can pull from.
type tableFeed struct {
	codec    keys.SQLCodec
	leaseMgr *lease.Manager
	rangefeed *rangefeed.RangeFeed
	tableID  descpb.ID

	mu struct {
		syncutil.Mutex
		cond   sync.Cond
		err    error
		buffer []bufferedEvent
	}
}

// WaitForEvents blocks until there is at least one event in the buffer, the
// context is cancelled, or an error occurs. The buffered event may data or it
// may be a resolved timestamp
func (t *tableFeed) WaitForEvents(ctx context.Context) error {
	panic("unimplemented")
}

// ReadBufferedEvents reads all buffered events. It returns an error if the
// tablefeed is in an unhealthy state.
// 
// TODO: add a max hlc to this.
func (t *tableFeed) ReadBufferedEvents(scratch []tree.Datums) ([]tree.Datums, checkpoint hlc.Timestamp) {
	t.mu.Lock()
	defer t.mu.Unlock()

	count := min(len(t.mu.buffer), cap(scratch))
	if count == 0 {
		return nil
	}

	copy(scratch[:count], t.mu.buffer[:count])
	t.mu.buffer = t.mu.buffer[count:]

	t.mu.cond.Broadcast()

	return scratch[:count]
}

// Close cleans up the rangefeed backing the tableFeed.
func (t *tableFeed) Close(ctx context.Context) error {
	// We set an error here because it may be necessary to unblock something
	// waiting in the condition variable.
	t.setFailed(ctx, errors.New("tableFeed closed"))
	return nil
}

func (t *tableFeed) onRangfeedValue(ctx context.Context, rfv *kvpb.RangeFeedValue) {
	err := func() error {
		row, err := t.decodeRangefeedValue(ctx, rfv)
		if err != nil {
			return errors.Wrap(err, "decoding rangefeed value")
		}

		t.mu.Lock()
		defer t.mu.Unlock()
		for {
			if len(t.mu.buffer) < maxBufSize {
				t.mu.buffer = append(t.mu.buffer, bufferedEvent{row: row})
				return nil
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}

			// TODO use vlog here
			log.Dev.Infof(ctx, "tablefeed buffer is full")

			t.mu.cond.Wait()
		}
	}()
	if err != nil {
		t.setFailed(ctx, err)
	}
}

func (t *tableFeed) onRangfeedCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
	if checkpoint.ResolvedTS.IsEmpty() {
		return
	}

	err := func() error {
		t.mu.Lock()
		defer t.mu.Unlock()
		for {
			if len(t.mu.buffer) < maxBufSize {
				t.mu.buffer = append(t.mu.buffer, bufferedEvent{resolved: checkpoint.ResolvedTS})
				return nil
			}

			if ctx.Err() != nil {
				return ctx.Err()
			}

			// TODO use vlog here
			log.Dev.Infof(ctx, "tablefeed buffer is full")

			t.mu.cond.Wait()
		}
		return nil
	}()
	if err != nil {
		t.setFailed(ctx, err)
	}
}

func (t *tableFeed) setFailed(ctx context.Context, err error) {
	// NOTE: it is important to close the rangefeed outside of the mutex because
	// Closing may block on one of the callbacks finishing.
	t.rangefeed.Close()

	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.err = err
	t.mu.cond.Broadcast()
}

func (t *tableFeed) tableDescriptor(ctx context.Context, ts hlc.Timestamp) (catalog.TableDescriptor, error) {
	// Retrieve the target TableDescriptor from the lease manager. No caching
	// is attempted because the lease manager does its own caching.
	desc, err := t.leaseMgr.Acquire(ctx, lease.TimestampToReadTimestamp(ts), t.tableID)
	if err != nil {
		// Manager can return all kinds of errors during chaos, but based on
		// its usage, none of them should ever be terminal.
		return nil, changefeedbase.MarkRetryableError(err)
	}
	tableDesc := desc.Underlying().(catalog.TableDescriptor)
	// Immediately release the lease, since we only need it for the exact
	// timestamp requested.
	desc.Release(ctx)
	if tableDesc.MaybeRequiresTypeHydration() {
		return nil, errors.AssertionFailedf("type hydration not supported yet")
	}
	return tableDesc, nil
	panic("unimplemented")
	return nil, nil
}

func (t *tableFeed) decodeRangefeedValue(ctx context.Context, rfv *kvpb.RangeFeedValue) (tree.Datums, error) {
	partialKey := rfv.Key
	partialKey, err := t.codec.StripTenantPrefix(partialKey)
	if err != nil {
		return nil, errors.Wrapf(err, "stripping tenant prefix: %s", keys.PrettyPrint(nil, partialKey))
	}

	familyID, err := keys.DecodeFamilyKey(partialKey)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding family key: %s", keys.PrettyPrint(nil, partialKey))
	}

	tableDesc, err := t.tableDescriptor(ctx, rfv.Value.Timestamp)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching table descriptor: %s", keys.PrettyPrint(nil, partialKey))
	}
	familyDesc, err := catalog.MustFindFamilyByID(tableDesc, descpb.FamilyID(familyID))
	if err != nil {
		return nil, errors.Wrapf(err, "fetching family descriptor: %s", keys.PrettyPrint(nil, partialKey))
	}
	cols, err := getRelevantColumnsForFamily(tableDesc, familyDesc)
	if err != nil {
		return nil, errors.Wrapf(err, "getting relevant columns for family: %s", keys.PrettyPrint(nil, partialKey))
	}

	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&spec, t.codec, tableDesc, tableDesc.GetPrimaryIndex(), cols); err != nil {
		return nil, errors.Wrapf(err, "initializing index fetch spec: %s", keys.PrettyPrint(nil, partialKey))
	}
	rf := row.Fetcher{}
	if err := rf.Init(ctx, row.FetcherInitArgs{
		Spec:              &spec,
		WillUseKVProvider: true,
		TraceKV:           true,
		TraceKVEvery:      &util.EveryN{N: 1},
	}); err != nil {
		return nil, errors.Wrapf(err, "initializing row fetcher: %s", keys.PrettyPrint(nil, partialKey))
	}
	kvProvider := row.KVProvider{KVs: []roachpb.KeyValue{{Key: rfv.Key, Value: rfv.Value}}}
	if err := rf.ConsumeKVProvider(ctx, &kvProvider); err != nil {
		return nil, errors.Wrapf(err, "consuming kv provider: %s", keys.PrettyPrint(nil, partialKey))
	}
	encDatums, _, err := rf.NextRow(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching next row: %s", keys.PrettyPrint(nil, partialKey))
	}
	_ = encDatums

	datums := make(tree.Datums, len(cols))
	for i, colID := range cols {
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, errors.Wrapf(err, "finding column by id: %d", colID)
		}
		ed := encDatums[i]
		if err := ed.EnsureDecoded(col.ColumnDesc().Type, &tree.DatumAlloc{}); err != nil {
			return nil, errors.Wrapf(err, "error decoding column %q as type %s", col.ColumnDesc().Name, col.ColumnDesc().Type.String())
		}
		datums[i] = ed.Datum
	}
	return datums, nil
}

func getRelevantColumnsForFamily(
	tableDesc catalog.TableDescriptor, familyDesc *descpb.ColumnFamilyDescriptor,
) ([]descpb.ColumnID, error) {
	cols := tableDesc.GetPrimaryIndex().CollectKeyColumnIDs()
	for _, colID := range familyDesc.ColumnIDs {
		cols.Add(colID)
	}

	// Maintain the ordering of tableDesc.PublicColumns(), which is
	// matches the order of columns in the SQL table.
	idx := 0
	result := make([]descpb.ColumnID, cols.Len())
	visibleColumns := tableDesc.PublicColumns()
	if tableDesc.GetDeclarativeSchemaChangerState() != nil {
		hasMergedIndex := catalog.HasDeclarativeMergedPrimaryIndex(tableDesc)
		visibleColumns = make([]catalog.Column, 0, cols.Len())
		for _, col := range tableDesc.AllColumns() {
			if col.Adding() {
				continue
			}
			if tableDesc.GetDeclarativeSchemaChangerState() == nil && !col.Public() {
				continue
			}
			if col.Dropped() && (!col.WriteAndDeleteOnly() || hasMergedIndex) {
				continue
			}
			visibleColumns = append(visibleColumns, col)
		}
		// Recover the order of the original columns.
		slices.SortStableFunc(visibleColumns, func(a, b catalog.Column) int {
			return int(a.GetPGAttributeNum()) - int(b.GetPGAttributeNum())
		})
	}
	for _, col := range visibleColumns {
		colID := col.GetID()
		if cols.Contains(colID) {
			result[idx] = colID
			idx++
		}
	}

	// Some columns in familyDesc.ColumnIDs may not be public, so
	// result may contain fewer columns than cols.
	result = result[:idx]
	return result, nil
}
