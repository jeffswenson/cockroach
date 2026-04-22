// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestUnresolvedTxnRecordQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	q := makeUnresolvedTxnRecordQueue()

	// Test empty queue.
	require.Equal(t, 0, q.Len())
	require.Nil(t, q.Oldest())

	// Add a transaction. Queue has one element.
	txn1 := uuid.MakeV4()
	txn1TS := hlc.Timestamp{WallTime: 10}
	q.Add(txn1, txn1TS)
	require.Equal(t, 1, q.Len())
	require.Equal(t, txn1, q.Oldest().txnID)
	require.Equal(t, txn1TS, q.Oldest().timestamp)

	// Add a second transaction at a higher timestamp. Oldest unchanged.
	txn2 := uuid.MakeV4()
	txn2TS := hlc.Timestamp{WallTime: 20}
	q.Add(txn2, txn2TS)
	require.Equal(t, 2, q.Len())
	require.Equal(t, txn1, q.Oldest().txnID)

	// Add a third transaction at a lower timestamp. New oldest.
	txn3 := uuid.MakeV4()
	txn3TS := hlc.Timestamp{WallTime: 5}
	q.Add(txn3, txn3TS)
	require.Equal(t, 3, q.Len())
	require.Equal(t, txn3, q.Oldest().txnID)
	require.Equal(t, txn3TS, q.Oldest().timestamp)

	// Timestamp tiebreaker: add two transactions with the same timestamp.
	// The one with the lexicographically smaller UUID should be oldest.
	tieQ := makeUnresolvedTxnRecordQueue()
	// Use deterministic UUIDs so we know their byte ordering.
	tieA, err := uuid.FromString("00000000-0000-0000-0000-000000000001")
	require.NoError(t, err)
	tieB, err := uuid.FromString("00000000-0000-0000-0000-000000000002")
	require.NoError(t, err)
	tieTS := hlc.Timestamp{WallTime: 42}
	// Add tieB first, then tieA. tieA should end up as oldest.
	tieQ.Add(tieB, tieTS)
	tieQ.Add(tieA, tieTS)
	require.Equal(t, 2, tieQ.Len())
	require.Equal(t, tieA, tieQ.Oldest().txnID)

	// Forward existing txn timestamp. txn3 is oldest at ts=5. Forward it to
	// ts=15. txn1 at ts=10 should become the new oldest.
	q.Add(txn3, hlc.Timestamp{WallTime: 15})
	require.Equal(t, 3, q.Len())
	require.Equal(t, txn1, q.Oldest().txnID)
	require.Equal(t, txn1TS, q.Oldest().timestamp)

	// Forward with an equal timestamp. No change.
	q.Add(txn1, txn1TS)
	require.Equal(t, txn1, q.Oldest().txnID)
	require.Equal(t, txn1TS, q.Oldest().timestamp)

	// Forward with a lower timestamp. No change (Forward is no-op).
	q.Add(txn1, hlc.Timestamp{WallTime: 1})
	require.Equal(t, txn1, q.Oldest().txnID)
	require.Equal(t, txn1TS, q.Oldest().timestamp)

	// Remove non-oldest. Returns false (not at heap top).
	wasMin := q.Remove(txn2)
	require.False(t, wasMin)
	require.Equal(t, 2, q.Len())
	require.Equal(t, txn1, q.Oldest().txnID)

	// Remove oldest. Returns true.
	wasMin = q.Remove(txn1)
	require.True(t, wasMin)
	require.Equal(t, 1, q.Len())
	require.Equal(t, txn3, q.Oldest().txnID)
	require.Equal(t, hlc.Timestamp{WallTime: 15}, q.Oldest().timestamp)

	// Remove non-existent. Returns false, no state change.
	wasMin = q.Remove(uuid.MakeV4())
	require.False(t, wasMin)
	require.Equal(t, 1, q.Len())

	// Remove last element. Queue empty.
	wasMin = q.Remove(txn3)
	require.True(t, wasMin)
	require.Equal(t, 0, q.Len())
	require.Nil(t, q.Oldest())
}

func TestResolvedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	rts := makeResolvedTimestamp()
	rts.Init(ctx)

	// Test empty resolved timestamp.
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Add a transaction record. No closed timestamp so no resolved timestamp.
	txn1 := uuid.MakeV4()
	rts.ConsumeRecordWritten(ctx, txn1, hlc.Timestamp{WallTime: 10})
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Add another transaction record. Still no closed timestamp.
	txn2 := uuid.MakeV4()
	rts.ConsumeRecordWritten(ctx, txn2, hlc.Timestamp{WallTime: 12})
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Set a closed timestamp. Resolved timestamp advances to the closed
	// timestamp, which is below the oldest unresolved txn.
	fwd := rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 5})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())

	// Forward closed timestamp beyond the oldest unresolved txn. Resolved
	// timestamp advances to Prev() of the oldest unresolved (txn1 at 10).
	// Prev() of {WallTime:10} = {WallTime:9, Logical:MaxInt32}, but logical
	// is truncated to 0, yielding {WallTime:9}.
	fwd = rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 15})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Forward the timestamp of txn1 from 10 to 20. After this, txn2 at
	// ts=12 is the oldest in the heap. ConsumeRecordWritten does not
	// trigger a recompute, so resolved stays at 9.
	rts.ConsumeRecordWritten(ctx, txn1, hlc.Timestamp{WallTime: 20})
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Commit txn1. It was forwarded to ts=20, so it is NOT the oldest (txn2
	// at ts=12 is). Remove returns false, no recompute. Resolved stays at 9.
	rts.ConsumeCommitted(ctx, txn1)
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Abort txn2 (the oldest). Recompute triggered. No unresolved txns left,
	// so resolved advances to closedTS=15.
	rts.ConsumeAborted(ctx, txn2)
	require.Equal(t, hlc.Timestamp{WallTime: 15}, rts.Get())

	// Forward closed timestamp with no unresolved txns. Resolved advances.
	fwd = rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 20})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 20}, rts.Get())
}

func TestResolvedTimestampNoClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	rts := makeResolvedTimestamp()
	rts.Init(ctx)

	// Add a transaction record. No closed timestamp so no resolved timestamp.
	txn1 := uuid.MakeV4()
	rts.ConsumeRecordWritten(ctx, txn1, hlc.Timestamp{WallTime: 1})
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Commit the transaction. Still no closed timestamp, still no resolved.
	rts.ConsumeCommitted(ctx, txn1)
	require.Equal(t, hlc.Timestamp{}, rts.Get())

	// Add and abort. Still no closed timestamp.
	txn2 := uuid.MakeV4()
	rts.ConsumeRecordWritten(ctx, txn2, hlc.Timestamp{WallTime: 3})
	require.Equal(t, hlc.Timestamp{}, rts.Get())
	rts.ConsumeAborted(ctx, txn2)
	require.Equal(t, hlc.Timestamp{}, rts.Get())
}

func TestResolvedTimestampNoUnresolvedTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	rts := makeResolvedTimestamp()
	rts.Init(ctx)

	// Set a closed timestamp. Resolved timestamp advances.
	fwd := rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 1})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 1}, rts.Get())

	// Forward closed timestamp. Resolved timestamp advances.
	fwd = rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 3})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, rts.Get())

	// Smaller closed timestamp. Resolved timestamp does not advance.
	fwd = rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 2})
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, rts.Get())

	// Equal closed timestamp. Resolved timestamp does not advance.
	fwd = rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 3})
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 3}, rts.Get())

	// Forward closed timestamp. Resolved timestamp advances.
	fwd = rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 4})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 4}, rts.Get())
}

func TestResolvedTimestampInit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	t.Run("CT Before Init", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Set a closed timestamp. Not initialized so no resolved timestamp.
		fwd := rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 5})
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Resolved timestamp moves to closed timestamp.
		fwd = rts.Init(ctx)
		require.True(t, fwd)
		require.Equal(t, hlc.Timestamp{WallTime: 5}, rts.Get())
	})
	t.Run("No CT Before Init", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Add a transaction. Not initialized so no resolved timestamp.
		txn1 := uuid.MakeV4()
		rts.ConsumeRecordWritten(ctx, txn1, hlc.Timestamp{WallTime: 3})
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Resolved timestamp still empty (no closedTS).
		fwd := rts.Init(ctx)
		require.False(t, fwd)
		require.Equal(t, hlc.Timestamp{}, rts.Get())
	})
	t.Run("Txn and CT Before Init", func(t *testing.T) {
		rts := makeResolvedTimestamp()

		// Add a transaction and set a closed timestamp before Init.
		txn1 := uuid.MakeV4()
		rts.ConsumeRecordWritten(ctx, txn1, hlc.Timestamp{WallTime: 3})
		rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 5})
		require.Equal(t, hlc.Timestamp{}, rts.Get())

		// Init. Resolved timestamp moves below the oldest unresolved txn.
		// min(5, Prev(3)) = Prev(3) = {WallTime:2, Logical:MaxInt32},
		// truncated to {WallTime:2}.
		fwd := rts.Init(ctx)
		require.True(t, fwd)
		require.Equal(t, hlc.Timestamp{WallTime: 2}, rts.Get())
	})
}

// TestResolvedTimestampLogicalTruncation verifies that the resolved timestamp
// truncates the logical clock component to zero.
func TestResolvedTimestampLogicalTruncation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	rts := makeResolvedTimestamp()
	rts.Init(ctx)

	// Set a closed timestamp with a non-zero logical part. Resolved timestamp
	// should have logical truncated to zero.
	fwd := rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 10, Logical: 2})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 10}, rts.Get())

	// Add an intent at a higher timestamp with logical part.
	txn1 := uuid.MakeV4()
	rts.ConsumeRecordWritten(ctx, txn1, hlc.Timestamp{WallTime: 10, Logical: 4})
	require.Equal(t, hlc.Timestamp{WallTime: 10}, rts.Get())

	// Forward closed timestamp. The unresolved txn's Prev() is
	// {WallTime:10, Logical:3}, truncated to {WallTime:10}. Since resolved is
	// already at WallTime:10, no advance.
	fwd = rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 11, Logical: 6})
	require.False(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 10}, rts.Get())

	// Commit the transaction. Resolved advances to closedTS with logical
	// truncated: {WallTime:11}.
	rts.ConsumeCommitted(ctx, txn1)
	require.Equal(t, hlc.Timestamp{WallTime: 11}, rts.Get())

	// Add a transaction one tick above the closed ts. Resolved timestamp
	// stays the same. This tests the case where the closed timestamp has a
	// logical part and a txn is in the next wall tick.
	txn2 := uuid.MakeV4()
	rts.ConsumeRecordWritten(ctx, txn2, hlc.Timestamp{WallTime: 12, Logical: 7})
	require.Equal(t, hlc.Timestamp{WallTime: 11}, rts.Get())
}

// TestResolvedTimestampForwardExistingTxn verifies that ConsumeRecordWritten
// with an already-tracked txnID forwards the timestamp and interacts correctly
// with the resolved timestamp.
func TestResolvedTimestampForwardExistingTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	rts := makeResolvedTimestamp()
	rts.Init(ctx)

	// Add two transactions, then set a closed timestamp.
	txnA := uuid.MakeV4()
	txnB := uuid.MakeV4()
	rts.ConsumeRecordWritten(ctx, txnA, hlc.Timestamp{WallTime: 10})
	rts.ConsumeRecordWritten(ctx, txnB, hlc.Timestamp{WallTime: 15})

	fwd := rts.ForwardClosedTS(ctx, hlc.Timestamp{WallTime: 30})
	require.True(t, fwd)
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Forward txnA's timestamp from 10 to 20. ConsumeRecordWritten does not
	// trigger a recompute, so resolved stays at 9.
	rts.ConsumeRecordWritten(ctx, txnA, hlc.Timestamp{WallTime: 20})
	require.Equal(t, hlc.Timestamp{WallTime: 9}, rts.Get())

	// Commit txnB (was at 15). txnA is now oldest at 20 (forwarded).
	// Resolved = min(30, Prev(20)) = 19.
	rts.ConsumeCommitted(ctx, txnB)
	require.Equal(t, hlc.Timestamp{WallTime: 19}, rts.Get())

	// Commit txnA. No unresolved txns. Resolved = closedTS = 30.
	rts.ConsumeCommitted(ctx, txnA)
	require.Equal(t, hlc.Timestamp{WallTime: 30}, rts.Get())
}
