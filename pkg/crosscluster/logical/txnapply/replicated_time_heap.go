// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import "github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"

// replicatedTimeHeap is a min-heap of TxnIDs ordered by timestamp. It tracks
// unapplied transactions so the applier can efficiently find the oldest one
// when computing replicated time. Resolved entries are lazily popped by
// callers rather than eagerly removed.
type replicatedTimeHeap []ldrdecoder.TxnID

func (h replicatedTimeHeap) Len() int { return len(h) }

func (h replicatedTimeHeap) Less(i, j int) bool {
	return h[i].Timestamp.Less(h[j].Timestamp)
}

func (h replicatedTimeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *replicatedTimeHeap) Push(x ldrdecoder.TxnID) { *h = append(*h, x) }

func (h *replicatedTimeHeap) Pop() ldrdecoder.TxnID {
	n := len(*h)
	item := (*h)[n-1]
	(*h)[n-1] = ldrdecoder.TxnID{} // avoid retaining references
	*h = (*h)[:n-1]
	return item
}

func (h *replicatedTimeHeap) peek() ldrdecoder.TxnID {
	if len(*h) == 0 {
		return ldrdecoder.TxnID{}
	}
	return (*h)[0]
}
