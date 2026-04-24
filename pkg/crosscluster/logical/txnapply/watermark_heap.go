// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// watermarkEntry represents either a transaction or a checkpoint in the
// watermark min-heap. The resolved time only advances when a checkpoint
// is popped from the heap, because a checkpoint at timestamp T guarantees
// that all transactions with timestamp <= T have been sent.
type watermarkEntry struct {
	timestamp    hlc.Timestamp
	txnID        ldrdecoder.TxnID // set for transaction entries only
	isCheckpoint bool
}

// watermarkHeap is a min-heap of watermarkEntries ordered by timestamp.
// When a checkpoint and a transaction share the same timestamp, the
// transaction sorts first to ensure we don't advance the resolved time
// past an unresolved transaction.
type watermarkHeap []watermarkEntry

func (h watermarkHeap) Len() int { return len(h) }

func (h watermarkHeap) Less(i, j int) bool {
	if h[i].timestamp.Equal(h[j].timestamp) {
		// Transactions before checkpoints at the same timestamp.
		return !h[i].isCheckpoint && h[j].isCheckpoint
	}
	return h[i].timestamp.Less(h[j].timestamp)
}

func (h watermarkHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *watermarkHeap) Push(x watermarkEntry) { *h = append(*h, x) }

func (h *watermarkHeap) Pop() watermarkEntry {
	n := len(*h)
	item := (*h)[n-1]
	(*h)[n-1] = watermarkEntry{} // avoid retaining references
	*h = (*h)[:n-1]
	return item
}

func (h *watermarkHeap) peek() watermarkEntry {
	if len(*h) == 0 {
		return watermarkEntry{}
	}
	return (*h)[0]
}
