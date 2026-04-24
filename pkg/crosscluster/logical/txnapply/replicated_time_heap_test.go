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

// TestReplicatedTimeHeapOrdering verifies that entries are popped in
// ascending timestamp order.
func TestReplicatedTimeHeapOrdering(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ids := []ldrdecoder.TxnID{
		{Timestamp: hlc.Timestamp{WallTime: 30}, ApplierID: 1},
		{Timestamp: hlc.Timestamp{WallTime: 10}, ApplierID: 1},
		{Timestamp: hlc.Timestamp{WallTime: 20}, ApplierID: 1},
	}

	var h replicatedTimeHeap
	for _, id := range ids {
		heap.Push(&h, id)
	}

	require.Equal(t, int64(10), heap.Pop(&h).Timestamp.WallTime)
	require.Equal(t, int64(20), heap.Pop(&h).Timestamp.WallTime)
	require.Equal(t, int64(30), heap.Pop(&h).Timestamp.WallTime)
	require.Equal(t, 0, h.Len())
}
