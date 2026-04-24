// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestWatermarkHeapTransactionBeforeCheckpoint verifies the tiebreaker in
// watermarkHeap.Less: when a transaction and a checkpoint share the same
// timestamp, the transaction must sort first. This prevents the resolved
// time from advancing past an unresolved transaction.
func TestWatermarkHeapTransactionBeforeCheckpoint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := hlc.Timestamp{WallTime: 10}
	txnEntry := watermarkEntry{
		timestamp: ts,
		txnID:     ldrdecoder.TxnID{Timestamp: ts, ApplierID: 1},
	}
	cpEntry := watermarkEntry{
		timestamp:    ts,
		isCheckpoint: true,
	}

	// Push checkpoint first, then transaction — the heap must still pop the
	// transaction before the checkpoint regardless of insertion order.
	var h watermarkHeap
	heap.Push(&h, cpEntry)
	heap.Push(&h, txnEntry)

	first := heap.Pop(&h)
	second := heap.Pop(&h)

	require.False(t, first.isCheckpoint,
		"expected transaction to be popped before checkpoint at equal timestamp")
	require.True(t, second.isCheckpoint,
		"expected checkpoint to be popped after transaction at equal timestamp")
	require.True(t, first.timestamp.Equal(second.timestamp),
		"both entries should have the same timestamp")
}

// TestWatermarkHeapTimestampOrdering verifies that entries are popped in
// ascending timestamp order, with the transaction-before-checkpoint
// tiebreaker applied at each timestamp.
func TestWatermarkHeapTimestampOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	entries := []watermarkEntry{
		{timestamp: hlc.Timestamp{WallTime: 20}, isCheckpoint: true},
		{timestamp: hlc.Timestamp{WallTime: 10}, txnID: ldrdecoder.TxnID{
			Timestamp: hlc.Timestamp{WallTime: 10}, ApplierID: 1,
		}},
		{timestamp: hlc.Timestamp{WallTime: 10}, isCheckpoint: true},
		{timestamp: hlc.Timestamp{WallTime: 20}, txnID: ldrdecoder.TxnID{
			Timestamp: hlc.Timestamp{WallTime: 20}, ApplierID: 1,
		}},
	}

	var h watermarkHeap
	for _, e := range entries {
		heap.Push(&h, e)
	}

	expected := []struct {
		wallTime     int64
		isCheckpoint bool
	}{
		{10, false}, // txn at ts=10
		{10, true},  // checkpoint at ts=10
		{20, false}, // txn at ts=20
		{20, true},  // checkpoint at ts=20
	}

	for i, want := range expected {
		got := heap.Pop(&h)
		require.Equal(t, want.wallTime, got.timestamp.WallTime,
			"entry %d: wrong timestamp", i)
		require.Equal(t, want.isCheckpoint, got.isCheckpoint,
			"entry %d: wrong type (isCheckpoint)", i)
	}
	require.Equal(t, 0, h.Len(), "heap should be empty after popping all entries")
}
