// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/container/heap"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// resolvedTimestamp tracks the resolved timestamp for the TxnFeed processor.
// The resolved timestamp is the minimum of the closed timestamp and the
// timestamp of the oldest unresolved transaction record in the tracked key
// range. An unresolved transaction record is one in a non-finalized state
// (PENDING or STAGING) that has not yet been committed or aborted.
//
// This is analogous to the rangefeed's resolvedTimestamp
// (pkg/kv/kvserver/rangefeed/resolved_timestamp.go), but simpler because
// transaction records are singular entities (no intent reference counting) and
// no TxnPusher is needed (the existing transaction lifecycle handles
// finalization).
type resolvedTimestamp struct {
	init       bool
	closedTS   hlc.Timestamp
	resolvedTS hlc.Timestamp
	txnQ       unresolvedTxnRecordQueue
}

func makeResolvedTimestamp() resolvedTimestamp {
	return resolvedTimestamp{
		txnQ: makeUnresolvedTxnRecordQueue(),
	}
}

// Init marks the resolved timestamp as initialized. Before initialization,
// ops are consumed to populate the queue but the resolved timestamp is not
// computed. After initialization, the resolved timestamp is computed and
// begins advancing. Returns whether the resolved timestamp moved forward.
func (rts *resolvedTimestamp) Init(ctx context.Context) bool {
	rts.init = true
	return rts.recompute(ctx)
}

// IsInit returns whether the resolved timestamp is initialized.
func (rts *resolvedTimestamp) IsInit() bool {
	return rts.init
}

// Get returns the current resolved timestamp.
func (rts *resolvedTimestamp) Get() hlc.Timestamp {
	return rts.resolvedTS
}

// ForwardClosedTS forwards the closed timestamp and recomputes the resolved
// timestamp. Returns whether the resolved timestamp moved forward.
func (rts *resolvedTimestamp) ForwardClosedTS(ctx context.Context, ts hlc.Timestamp) bool {
	if rts.closedTS.Forward(ts) {
		return rts.recompute(ctx)
	}
	return false
}

// ConsumeRecordWritten tracks a non-finalized transaction record. If the
// transaction is already tracked, its timestamp is forwarded.
func (rts *resolvedTimestamp) ConsumeRecordWritten(
	ctx context.Context, txnID uuid.UUID, writeTS hlc.Timestamp,
) {
	rts.assertOpAboveRTS(ctx, writeTS)
	rts.txnQ.Add(txnID, writeTS)
	// Adding or forwarding a record can never advance the resolved timestamp.
}

// ConsumeCommitted removes a committed transaction from tracking.
func (rts *resolvedTimestamp) ConsumeCommitted(ctx context.Context, txnID uuid.UUID) {
	if rts.txnQ.Remove(txnID) {
		rts.recompute(ctx)
	}
}

// ConsumeAborted removes an aborted transaction from tracking.
func (rts *resolvedTimestamp) ConsumeAborted(ctx context.Context, txnID uuid.UUID) {
	if rts.txnQ.Remove(txnID) {
		rts.recompute(ctx)
	}
}

// recompute computes the resolved timestamp based on the closed timestamp and
// the oldest unresolved transaction record. Returns whether the resolved
// timestamp moved forward.
func (rts *resolvedTimestamp) recompute(ctx context.Context) bool {
	if !rts.init {
		return false
	}
	if rts.closedTS.Less(rts.resolvedTS) {
		log.KvExec.Fatalf(ctx,
			"closed timestamp below resolved timestamp: %s < %s",
			rts.closedTS, rts.resolvedTS)
	}
	newTS := rts.closedTS

	if txn := rts.txnQ.Oldest(); txn != nil {
		if txn.timestamp.LessEq(rts.resolvedTS) {
			log.KvExec.Fatalf(ctx,
				"unresolved txn record equal to or below resolved timestamp: %s <= %s",
				txn.timestamp, rts.resolvedTS)
		}
		txnTS := txn.timestamp.Prev()
		newTS.Backward(txnTS)
	}

	// Truncate the logical part to avoid pushing things above Logical=MaxInt32.
	newTS.Logical = 0

	if newTS.Less(rts.resolvedTS) {
		log.KvExec.Fatalf(ctx,
			"resolved timestamp regression, was %s, recomputed as %s",
			rts.resolvedTS, newTS)
	}
	return rts.resolvedTS.Forward(newTS)
}

// assertOpAboveRTS asserts that the given timestamp is above the current
// resolved timestamp.
func (rts *resolvedTimestamp) assertOpAboveRTS(ctx context.Context, ts hlc.Timestamp) {
	if rts.init && ts.LessEq(rts.resolvedTS) {
		log.KvExec.Fatalf(ctx,
			"txn record write timestamp %s at or below resolved timestamp %s",
			ts, rts.resolvedTS)
	}
}

// unresolvedTxnRecord represents a transaction with a non-finalized record on
// this range. The timestamp is the transaction's WriteTimestamp, which
// represents when the transaction's effects are visible.
type unresolvedTxnRecord struct {
	txnID     uuid.UUID
	timestamp hlc.Timestamp
	index     int // position in heap, maintained by heap.Interface
}

// unresolvedTxnRecordHeap implements heap.Interface, ordered by timestamp so
// the oldest unresolved transaction rises to the top.
type unresolvedTxnRecordHeap []*unresolvedTxnRecord

func (h unresolvedTxnRecordHeap) Len() int { return len(h) }

func (h unresolvedTxnRecordHeap) Less(i, j int) bool {
	if h[i].timestamp == h[j].timestamp {
		return bytes.Compare(h[i].txnID.GetBytes(), h[j].txnID.GetBytes()) < 0
	}
	return h[i].timestamp.Less(h[j].timestamp)
}

func (h unresolvedTxnRecordHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

func (h *unresolvedTxnRecordHeap) Push(rec *unresolvedTxnRecord) {
	rec.index = len(*h)
	*h = append(*h, rec)
}

func (h *unresolvedTxnRecordHeap) Pop() *unresolvedTxnRecord {
	old := *h
	n := len(old)
	rec := old[n-1]
	rec.index = -1
	old[n-1] = nil
	*h = old[:n-1]
	return rec
}

// unresolvedTxnRecordQueue tracks all unresolved (non-finalized) transaction
// records within a key range. It maintains a min-heap ordered by
// WriteTimestamp so that the oldest unresolved transaction can be found in O(1)
// time. Combined with the closed timestamp, this allows computing a resolved
// timestamp.
type unresolvedTxnRecordQueue struct {
	txns    map[uuid.UUID]*unresolvedTxnRecord
	minHeap unresolvedTxnRecordHeap
}

func makeUnresolvedTxnRecordQueue() unresolvedTxnRecordQueue {
	return unresolvedTxnRecordQueue{
		txns: make(map[uuid.UUID]*unresolvedTxnRecord),
	}
}

// Len returns the number of transactions being tracked.
func (q *unresolvedTxnRecordQueue) Len() int {
	return q.minHeap.Len()
}

// Oldest returns the oldest unresolved transaction record, or nil if empty.
func (q *unresolvedTxnRecordQueue) Oldest() *unresolvedTxnRecord {
	if q.Len() == 0 {
		return nil
	}
	return q.minHeap[0]
}

// Add adds a transaction to the queue or forwards its timestamp if already
// tracked.
func (q *unresolvedTxnRecordQueue) Add(txnID uuid.UUID, writeTS hlc.Timestamp) {
	if rec, ok := q.txns[txnID]; ok {
		if rec.timestamp.Forward(writeTS) {
			heap.Fix[*unresolvedTxnRecord](&q.minHeap, rec.index)
		}
		return
	}
	rec := &unresolvedTxnRecord{
		txnID:     txnID,
		timestamp: writeTS,
	}
	q.txns[txnID] = rec
	heap.Push[*unresolvedTxnRecord](&q.minHeap, rec)
}

// Remove removes a transaction from the queue. Returns true if the removal
// could have advanced the oldest timestamp (i.e. the removed transaction was
// at the top of the heap).
func (q *unresolvedTxnRecordQueue) Remove(txnID uuid.UUID) bool {
	rec, ok := q.txns[txnID]
	if !ok {
		return false
	}
	wasMin := rec.index == 0
	delete(q.txns, txnID)
	heap.Remove[*unresolvedTxnRecord](&q.minHeap, rec.index)
	return wasMin
}
