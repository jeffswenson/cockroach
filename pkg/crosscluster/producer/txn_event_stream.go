// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// txnEventStream implements eval.ValueGenerator for the
// crdb_internal.txn_feed_partition builtin. It wraps the DistSender TxnFeed
// and delivers batches of TxnFeedMessages using a no-linger batching
// strategy: the subscriber blocks until data is available, then receives
// all accumulated events in one batch.
type txnEventStream struct {
	spec    streampb.TxnFeedPartitionSpec
	execCfg *sql.ExecutorConfig

	// streamCh carries serialized TxnFeedBatch datums from the batch
	// goroutine to Next(). errCh carries a fatal error from the feed or
	// batch goroutine.
	streamCh chan tree.Datums
	errCh    chan error
	data     tree.Datums

	group  ctxgroup.Group
	cancel context.CancelFunc
}

var _ eval.ValueGenerator = (*txnEventStream)(nil)

var txnEventStreamReturnType = types.MakeLabeledTuple(
	[]*types.T{types.Bytes},
	[]string{"txn_feed_event"},
)

// ResolvedType implements eval.ValueGenerator.
func (s *txnEventStream) ResolvedType() *types.T {
	return txnEventStreamReturnType
}

// Start implements eval.ValueGenerator.
func (s *txnEventStream) Start(ctx context.Context, _ *kv.Txn) error {
	if s.errCh != nil {
		return errors.AssertionFailedf("expected to be started once")
	}

	s.errCh = make(chan error, 1)
	s.streamCh = make(chan tree.Datums)

	ds := s.execCfg.DistSQLSrv.DistSender

	// Build SpanTimePairs and TxnFeedOptions from the spec.
	spans := make([]kvcoord.SpanTimePair, len(s.spec.Spans))
	for i, sp := range s.spec.Spans {
		spans[i] = kvcoord.SpanTimePair{
			Span:       sp.Span,
			StartAfter: sp.Timestamp,
		}
	}
	var opts []kvcoord.TxnFeedOption
	if s.spec.Enrich {
		opts = append(opts, kvcoord.WithEnrichment())
	}
	if len(s.spec.DetailSpans) > 0 {
		opts = append(opts, kvcoord.WithDetailSpans(s.spec.DetailSpans))
	}
	if len(s.spec.DepOnlySpans) > 0 {
		opts = append(opts,
			kvcoord.WithDependencyOnlySpans(s.spec.DepOnlySpans))
	}

	// rawCh receives messages from the TxnFeed. It is buffered so that
	// the TxnFeed can continue producing while the batch goroutine
	// serializes and delivers the previous batch.
	rawCh := make(chan kvpb.TxnFeedMessage, 4096)

	ctx, s.cancel = context.WithCancel(ctx)
	s.group = ctxgroup.WithContext(ctx)

	// Feed goroutine: runs ds.TxnFeed which blocks until ctx is
	// cancelled or a fatal error occurs.
	s.group.GoCtx(func(ctx context.Context) error {
		defer close(rawCh)
		err := ds.TxnFeed(ctx, spans, rawCh, opts...)
		if err != nil {
			select {
			case s.errCh <- err:
			default:
			}
		}
		return err
	})

	// Batch goroutine: reads from rawCh using a no-linger batch pattern
	// and sends serialized TxnFeedBatch datums on streamCh.
	s.group.GoCtx(func(ctx context.Context) error {
		defer close(s.streamCh)
		err := runTxnFeedBatcher(ctx, rawCh, s.streamCh)
		if err != nil {
			select {
			case s.errCh <- err:
			default:
			}
		}
		return err
	})

	return nil
}

// runTxnFeedBatcher reads TxnFeedMessages from rawCh and delivers
// serialized TxnFeedBatch datums on streamCh. It blocks until at least
// one message is available, then non-blocking drains all remaining
// messages before serializing and sending the batch.
func runTxnFeedBatcher(
	ctx context.Context, rawCh <-chan kvpb.TxnFeedMessage, streamCh chan<- tree.Datums,
) error {
	var batch []kvpb.TxnFeedMessage
	for {
		// Block until at least one message arrives.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-rawCh:
			if !ok {
				return nil
			}
			batch = append(batch, msg)
		}

		// Non-blocking drain: grab everything that's ready.
	drain:
		for {
			select {
			case msg, ok := <-rawCh:
				if !ok {
					break drain
				}
				batch = append(batch, msg)
			default:
				break drain
			}
		}

		// Serialize and send.
		data, err := marshalTxnFeedBatch(batch)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case streamCh <- data:
		}
		batch = batch[:0]
	}
}

// marshalTxnFeedBatch serializes a slice of TxnFeedMessages into a
// TxnFeedBatch wrapped in tree.Datums.
func marshalTxnFeedBatch(msgs []kvpb.TxnFeedMessage) (tree.Datums, error) {
	batch := streampb.TxnFeedBatch{Messages: msgs}
	data, err := protoutil.Marshal(&batch)
	if err != nil {
		return nil, errors.Wrap(err, "marshaling TxnFeedBatch")
	}
	return tree.Datums{tree.NewDBytes(tree.DBytes(data))}, nil
}

// Next implements eval.ValueGenerator.
func (s *txnEventStream) Next(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case err := <-s.errCh:
		return false, err
	case d, ok := <-s.streamCh:
		if !ok {
			// Check for an error that arrived after the channel closed.
			select {
			case err := <-s.errCh:
				return false, err
			default:
				return false, nil
			}
		}
		s.data = d
		return true, nil
	}
}

// Values implements eval.ValueGenerator.
func (s *txnEventStream) Values() (tree.Datums, error) {
	return s.data, nil
}

// Close implements eval.ValueGenerator.
func (s *txnEventStream) Close(ctx context.Context) {
	if s.cancel == nil {
		return
	}
	s.cancel()
	if err := s.group.Wait(); err != nil &&
		!errors.Is(err, context.Canceled) {
		log.Dev.Errorf(ctx, "txn feed stream terminated with error %v", err)
	}
}

// txnFeedPartition creates a txnEventStream ValueGenerator from a
// serialized TxnFeedPartitionSpec. Called by
// replicationStreamManagerImpl.TxnFeedPartition.
func txnFeedPartition(
	evalCtx *eval.Context, streamID streampb.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	var spec streampb.TxnFeedPartitionSpec
	if err := protoutil.Unmarshal(opaqueSpec, &spec); err != nil {
		return nil, errors.Wrapf(err,
			"invalid txn feed partition spec for stream %d", streamID)
	}
	if len(spec.Spans) == 0 {
		return nil, errors.AssertionFailedf(
			"expected at least one span, got none")
	}

	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)
	return &txnEventStream{
		spec:    spec,
		execCfg: execCfg,
	}, nil
}
