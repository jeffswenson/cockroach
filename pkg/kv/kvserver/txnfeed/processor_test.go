// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// testStream is a mock Stream implementation that captures events for
// assertions.
type testStream struct {
	mu struct {
		syncutil.Mutex
		events []*kvpb.TxnFeedEvent
		err    *kvpb.Error
	}
}

var _ Stream = (*testStream)(nil)

func (s *testStream) SendBuffered(event *kvpb.TxnFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.events = append(s.mu.events, event)
	return nil
}

func (s *testStream) SendUnbuffered(event *kvpb.TxnFeedEvent) error {
	return s.SendBuffered(event)
}

func (s *testStream) SendError(err *kvpb.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.err = err
}

func (s *testStream) getAndClearEvents() []*kvpb.TxnFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.mu.events
	s.mu.events = nil
	return events
}

func (s *testStream) getError() *kvpb.Error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mu.err
}

func newTestProcessor(t *testing.T, stopper *stop.Stopper) (*Processor, storage.Engine) {
	t.Helper()
	eng := storage.NewDefaultInMemForTesting()

	p := NewProcessor(Config{
		AmbientContext: log.MakeTestingAmbientCtxWithNewTracer(),
		Span: roachpb.RSpan{
			Key:    roachpb.RKey("a"),
			EndKey: roachpb.RKey("z"),
		},
		Stopper: stopper,
	})

	snap := eng.NewSnapshot()
	defer snap.Close()
	require.NoError(t, p.Init(context.Background(), snap))

	return p, eng
}

// TestProcessorResolvedTimestampCheckpoints tests the end-to-end flow of
// resolved timestamp advancement and checkpoint emission.
func TestProcessorResolvedTimestampCheckpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p, eng := newTestProcessor(t, stopper)
	defer eng.Close()

	// Register a stream.
	stream := &testStream{}
	_, err := p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{}, /* startTS — empty means no catch-up */
		nil,             /* snap — nil when no catch-up */
		stream,
	)
	require.NoError(t, err)
	stream.getAndClearEvents() // discard any initial events

	// Forward closed timestamp. Stream should receive a checkpoint.
	p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 10})
	events := stream.getAndClearEvents()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Checkpoint)
	require.Equal(t,
		hlc.Timestamp{WallTime: 10}, events[0].Checkpoint.ResolvedTS)

	// Add an unresolved transaction record at ts=15.
	txn1 := uuid.MakeV4()
	p.ConsumeTxnFeedOps(ctx, &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:           kvserverpb.TxnFeedOp_RECORD_WRITTEN,
			TxnID:          txn1,
			AnchorKey:      roachpb.Key("b"),
			WriteTimestamp: hlc.Timestamp{WallTime: 15},
		}},
	})
	// RECORD_WRITTEN may or may not emit a checkpoint depending on whether
	// resolved TS changed. It shouldn't have regressed, and the closed TS
	// is still 10 which is below the txn, so resolved stays at 10.
	stream.getAndClearEvents()

	// Forward closed timestamp to 20. Resolved should advance to
	// min(20, Prev(15)) = 14.
	p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 20})
	events = stream.getAndClearEvents()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Checkpoint)
	require.Equal(t,
		hlc.Timestamp{WallTime: 14}, events[0].Checkpoint.ResolvedTS)

	// Commit txn1. No more unresolved txns. Resolved advances to
	// closedTS=20.
	p.ConsumeTxnFeedOps(ctx, &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:           kvserverpb.TxnFeedOp_COMMITTED,
			TxnID:          txn1,
			AnchorKey:      roachpb.Key("b"),
			WriteTimestamp: hlc.Timestamp{WallTime: 15},
			WriteSpans:     []roachpb.Span{{Key: roachpb.Key("x")}},
		}},
	})
	events = stream.getAndClearEvents()
	// Should have both a committed event and a checkpoint.
	var gotCommitted, gotCheckpoint bool
	for _, ev := range events {
		if ev.Committed != nil {
			gotCommitted = true
			require.Equal(t, txn1, ev.Committed.TxnID)
		}
		if ev.Checkpoint != nil {
			gotCheckpoint = true
			require.Equal(t,
				hlc.Timestamp{WallTime: 20}, ev.Checkpoint.ResolvedTS)
		}
	}
	require.True(t, gotCommitted, "expected committed event")
	require.True(t, gotCheckpoint, "expected checkpoint event")
}

// TestProcessorConsumeTxnFeedOps tests event dispatch to registrations.
func TestProcessorConsumeTxnFeedOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p, eng := newTestProcessor(t, stopper)
	defer eng.Close()

	// Register a stream for span [a, m).
	stream := &testStream{}
	_, err := p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{}, nil, stream,
	)
	require.NoError(t, err)
	stream.getAndClearEvents()

	// Committed op with anchor in span -> dispatched.
	txn1 := uuid.MakeV4()
	p.ConsumeTxnFeedOps(ctx, &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:           kvserverpb.TxnFeedOp_COMMITTED,
			TxnID:          txn1,
			AnchorKey:      roachpb.Key("c"),
			WriteTimestamp: hlc.Timestamp{WallTime: 5},
			WriteSpans:     []roachpb.Span{{Key: roachpb.Key("c")}},
		}},
	})
	events := stream.getAndClearEvents()
	var gotCommitted bool
	for _, ev := range events {
		if ev.Committed != nil {
			gotCommitted = true
			require.Equal(t, txn1, ev.Committed.TxnID)
			require.Equal(t, roachpb.Key("c"), ev.Committed.AnchorKey)
		}
	}
	require.True(t, gotCommitted, "expected committed event for key in span")

	// Committed op with anchor outside span -> not dispatched.
	txn2 := uuid.MakeV4()
	p.ConsumeTxnFeedOps(ctx, &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:           kvserverpb.TxnFeedOp_COMMITTED,
			TxnID:          txn2,
			AnchorKey:      roachpb.Key("z"),
			WriteTimestamp: hlc.Timestamp{WallTime: 6},
		}},
	})
	events = stream.getAndClearEvents()
	for _, ev := range events {
		require.Nil(t, ev.Committed,
			"should not receive committed event for key outside span")
	}

	// RECORD_WRITTEN does not dispatch events to registrations.
	txn3 := uuid.MakeV4()
	p.ConsumeTxnFeedOps(ctx, &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:           kvserverpb.TxnFeedOp_RECORD_WRITTEN,
			TxnID:          txn3,
			AnchorKey:      roachpb.Key("c"),
			WriteTimestamp: hlc.Timestamp{WallTime: 7},
		}},
	})
	events = stream.getAndClearEvents()
	for _, ev := range events {
		require.Nil(t, ev.Committed,
			"RECORD_WRITTEN should not produce committed events")
	}

	// ABORTED does not dispatch events to registrations.
	p.ConsumeTxnFeedOps(ctx, &kvserverpb.TxnFeedOps{
		Ops: []kvserverpb.TxnFeedOp{{
			Type:      kvserverpb.TxnFeedOp_ABORTED,
			TxnID:     txn3,
			AnchorKey: roachpb.Key("c"),
		}},
	})
	events = stream.getAndClearEvents()
	for _, ev := range events {
		require.Nil(t, ev.Committed,
			"ABORTED should not produce committed events")
	}

	// Nil ops are a no-op.
	p.ConsumeTxnFeedOps(ctx, nil)
	events = stream.getAndClearEvents()
	require.Empty(t, events)
}

// TestProcessorInitWithUnresolvedRecords tests that Init correctly scans
// existing unresolved records and accounts for them in the resolved timestamp.
func TestProcessorInitWithUnresolvedRecords(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write a PENDING transaction record at ts=5.
	txn1ID := uuid.MakeV4()
	anchorKey1 := roachpb.Key("b")
	txn1 := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            anchorKey1,
			ID:             txn1ID,
			WriteTimestamp: hlc.Timestamp{WallTime: 5},
		},
		Status: roachpb.PENDING,
	}
	txnKey1 := keys.TransactionKey(anchorKey1, txn1ID)
	require.NoError(t, storage.MVCCPutProto(
		ctx, eng, txnKey1, hlc.Timestamp{WallTime: 10}, &txn1,
		storage.MVCCWriteOptions{},
	))

	// Write a STAGING transaction record at ts=8.
	txn2ID := uuid.MakeV4()
	anchorKey2 := roachpb.Key("d")
	txn2 := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            anchorKey2,
			ID:             txn2ID,
			WriteTimestamp: hlc.Timestamp{WallTime: 8},
		},
		Status: roachpb.STAGING,
	}
	txnKey2 := keys.TransactionKey(anchorKey2, txn2ID)
	require.NoError(t, storage.MVCCPutProto(
		ctx, eng, txnKey2, hlc.Timestamp{WallTime: 15}, &txn2,
		storage.MVCCWriteOptions{},
	))

	// Create and init processor.
	p := NewProcessor(Config{
		AmbientContext: log.MakeTestingAmbientCtxWithNewTracer(),
		Span: roachpb.RSpan{
			Key:    roachpb.RKey("a"),
			EndKey: roachpb.RKey("z"),
		},
		Stopper: stopper,
	})
	snap := eng.NewSnapshot()
	defer snap.Close()
	require.NoError(t, p.Init(ctx, snap))

	// Register a stream.
	stream := &testStream{}
	_, err := p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{}, nil, stream,
	)
	require.NoError(t, err)
	stream.getAndClearEvents()

	// Forward closed timestamp to 10. Resolved should be
	// min(10, Prev(5)) = 4 because txn1 at ts=5 is the oldest.
	p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 10})
	events := stream.getAndClearEvents()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Checkpoint)
	require.Equal(t,
		hlc.Timestamp{WallTime: 4}, events[0].Checkpoint.ResolvedTS)
}

// TestProcessorCheckpointNotDuplicated verifies that the same resolved
// timestamp is not emitted as a checkpoint twice.
func TestProcessorCheckpointNotDuplicated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p, eng := newTestProcessor(t, stopper)
	defer eng.Close()

	stream := &testStream{}
	_, err := p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{}, nil, stream,
	)
	require.NoError(t, err)
	stream.getAndClearEvents()

	// First forward: checkpoint emitted.
	p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 10})
	events := stream.getAndClearEvents()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Checkpoint)

	// Same timestamp again: no duplicate checkpoint.
	p.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 10})
	events = stream.getAndClearEvents()
	require.Empty(t, events)
}

// TestProcessorStopDisconnectsRegistrations verifies that stopping the
// processor disconnects all active registrations.
func TestProcessorStopDisconnectsRegistrations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p, eng := newTestProcessor(t, stopper)
	defer eng.Close()

	stream1 := &testStream{}
	stream2 := &testStream{}
	_, err := p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{}, nil, stream1,
	)
	require.NoError(t, err)
	_, err = p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("n"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{}, nil, stream2,
	)
	require.NoError(t, err)
	require.Equal(t, 2, p.Len())

	// Stop the processor with a specific error.
	stopErr := kvpb.NewErrorf("processor stopped")
	p.StopWithErr(stopErr)

	// Both streams should have received the error.
	require.NotNil(t, stream1.getError())
	require.NotNil(t, stream2.getError())
	require.Equal(t, 0, p.Len())
}

// TestProcessorDisconnectSpanWithErr verifies that DisconnectSpanWithErr
// disconnects only registrations overlapping the given span.
func TestProcessorDisconnectSpanWithErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	p, eng := newTestProcessor(t, stopper)
	defer eng.Close()

	stream1 := &testStream{}
	stream2 := &testStream{}
	_, err := p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("a"), EndKey: roachpb.RKey("m")},
		hlc.Timestamp{}, nil, stream1,
	)
	require.NoError(t, err)
	_, err = p.Register(ctx,
		roachpb.RSpan{Key: roachpb.RKey("n"), EndKey: roachpb.RKey("z")},
		hlc.Timestamp{}, nil, stream2,
	)
	require.NoError(t, err)
	require.Equal(t, 2, p.Len())

	// Disconnect span [a, f) which overlaps only the first registration.
	p.DisconnectSpanWithErr(
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")},
		kvpb.NewErrorf("test error"),
	)

	require.NotNil(t, stream1.getError())
	require.Nil(t, stream2.getError())
	require.Equal(t, 1, p.Len())
}
