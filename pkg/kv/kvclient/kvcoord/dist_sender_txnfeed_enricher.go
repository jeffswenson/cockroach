// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	spanpkg "github.com/cockroachdb/cockroach/pkg/util/span"
)

const enrichBatchSize = 64

// runTxnFeedEnricher reads events from rawCh, enriches committed
// events by calling GetTxnDetails in batches, and forwards all events
// to consumerCh. Checkpoints are used to track the dependency cutoff
// frontier.
func runTxnFeedEnricher(
	ctx context.Context,
	ds *DistSender,
	spans []SpanTimePair,
	detailSpans []roachpb.Span,
	depOnlySpans []roachpb.Span,
	rawCh <-chan kvpb.TxnFeedMessage,
	consumerCh chan<- kvpb.TxnFeedMessage,
) error {
	registeredSpans := make([]roachpb.Span, len(spans))
	for i, stp := range spans {
		registeredSpans[i] = stp.Span
	}
	frontier, err := spanpkg.MakeFrontier(registeredSpans...)
	if err != nil {
		return err
	}
	defer frontier.Release()

	// Initialize the frontier with the registration timestamps so that
	// the dependency cutoff for the first batch of events reflects the
	// actual start time rather than zero. A zero cutoff causes
	// GetTxnDetails to set EventHorizon = CommitTimestamp, which creates
	// a deadlock: the applier cannot apply the transaction until the
	// frontier reaches EventHorizon, but the frontier cannot advance
	// past the unapplied transaction.
	for _, stp := range spans {
		if _, err := frontier.Forward(stp.Span, stp.StartAfter); err != nil {
			return err
		}
	}

	batch := make([]kvpb.TxnFeedMessage, 0, enrichBatchSize)
	for {
		// Block until at least one event is available.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-rawCh:
			if !ok {
				return nil
			}
			batch = append(batch, msg)
		}

		// Non-blocking drain for more events up to batch size.
		for len(batch) < enrichBatchSize {
			select {
			case msg, ok := <-rawCh:
				if !ok {
					goto processBatch
				}
				batch = append(batch, msg)
			default:
				goto processBatch
			}
		}

	processBatch:
		for i := range batch {
			logTxnFeedMessage(ctx, "enricher-raw", &batch[i])
		}
		batch = pruneIrrelevantCommits(batch, detailSpans)
		if err := enrichBatch(ctx, ds, frontier, batch, detailSpans, depOnlySpans); err != nil {
			return err
		}

		// Forward all events to consumer in order.
		for i := range batch {
			logTxnFeedMessage(ctx, "enricher-out", &batch[i])
			select {
			case <-ctx.Done():
				return ctx.Err()
			case consumerCh <- batch[i]:
			}
		}

		// Update frontier from checkpoints (for next batch's cutoff).
		for _, msg := range batch {
			if msg.Event.Checkpoint != nil {
				if _, err := frontier.Forward(
					msg.Event.Checkpoint.AnchorSpan, msg.Event.Checkpoint.ResolvedTS,
				); err != nil {
					return err
				}
			}
		}

		batch = batch[:0]
	}
}

// pruneIrrelevantCommits removes committed events whose write and
// read spans have no overlap with the detail spans. These are
// internal transactions (e.g. the async make-explicit-commit
// transaction) that only touch transaction record keys and carry no
// user data. Checkpoint events are always kept.
func pruneIrrelevantCommits(
	batch []kvpb.TxnFeedMessage, detailSpans []roachpb.Span,
) []kvpb.TxnFeedMessage {
	if len(detailSpans) == 0 {
		return batch
	}
	n := 0
	for i := range batch {
		if c := batch[i].Event.Committed; c != nil {
			if !anySpanOverlaps(c.WriteSpans, detailSpans) &&
				!anySpanOverlaps(c.ReadSpans, detailSpans) {
				continue
			}
		}
		batch[n] = batch[i]
		n++
	}
	return batch[:n]
}

// anySpanOverlaps returns true if any span in spans overlaps with any
// span in filter.
func anySpanOverlaps(spans []roachpb.Span, filter []roachpb.Span) bool {
	for _, s := range spans {
		for _, f := range filter {
			if s.Overlaps(f) {
				return true
			}
		}
	}
	return false
}

// enrichBatch builds a single BatchRequest with one
// GetTxnDetailsRequest per committed event in the batch and populates
// each committed event's Details field from the response.
func enrichBatch(
	ctx context.Context,
	ds *DistSender,
	frontier spanpkg.ReadOnlyFrontier,
	batch []kvpb.TxnFeedMessage,
	detailSpans []roachpb.Span,
	depOnlySpans []roachpb.Span,
) error {
	var commitIndices []int
	for i := range batch {
		if batch[i].Event.Committed != nil {
			commitIndices = append(commitIndices, i)
		}
	}
	if len(commitIndices) == 0 {
		return nil
	}

	depCutoff := frontier.Frontier()

	ba := &kvpb.BatchRequest{}
	for _, idx := range commitIndices {
		c := batch[idx].Event.Committed
		writeSpans := filterOverlappingSpans(c.WriteSpans, detailSpans)
		// Read spans are not filtered by detailSpans. They represent
		// the transaction's actual read footprint from the txn record
		// and are needed for complete dependency tracking regardless
		// of which tables LDR is watching.
		readSpans := c.ReadSpans
		readSpans, writeSpans = moveDepOnlyWrites(readSpans, writeSpans, depOnlySpans)
		allSpans := slices.Concat(writeSpans, readSpans)

		minKey, maxEndKey := computeRequestBounds(allSpans)

		ba.Add(&kvpb.GetTxnDetailsRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    minKey,
				EndKey: maxEndKey,
			},
			TxnID:            c.TxnID,
			CommitTimestamp:  c.CommitTimestamp,
			DependencyCutoff: depCutoff,
			ReadSpans:        readSpans,
			WriteSpans:       writeSpans,
		})
	}

	br, pErr := ds.Send(ctx, ba)
	if pErr != nil {
		return pErr.GoError()
	}

	for i, idx := range commitIndices {
		resp := br.Responses[i].GetGetTxnDetails()
		batch[idx].Details = &kvpb.TxnFeedDetails{
			Writes:       resp.Writes,
			Dependencies: resp.Dependencies,
			EventHorizon: resp.EventHorizon,
		}
	}
	return nil
}

// moveDepOnlyWrites moves write spans that are fully contained by a
// dependency-only span into the read set. Write spans that only
// partially overlap (or don't overlap at all) stay as writes.
func moveDepOnlyWrites(
	readSpans, writeSpans []roachpb.Span, depOnlySpans []roachpb.Span,
) ([]roachpb.Span, []roachpb.Span) {
	if len(depOnlySpans) == 0 {
		return readSpans, writeSpans
	}
	var keptWrites []roachpb.Span
	for _, ws := range writeSpans {
		if spanContainedByAny(ws, depOnlySpans) {
			readSpans = append(readSpans, ws)
		} else {
			keptWrites = append(keptWrites, ws)
		}
	}
	return readSpans, keptWrites
}

func spanContainedByAny(s roachpb.Span, containers []roachpb.Span) bool {
	for _, c := range containers {
		if c.Contains(s) {
			return true
		}
	}
	return false
}

// filterOverlappingSpans returns the subset of spans that overlap with
// at least one of the filter spans. If filter is empty, all spans are
// returned unmodified.
func filterOverlappingSpans(spans []roachpb.Span, filter []roachpb.Span) []roachpb.Span {
	if len(filter) == 0 {
		return spans
	}
	var out []roachpb.Span
	for _, s := range spans {
		for _, f := range filter {
			if s.Overlaps(f) {
				out = append(out, s)
				break
			}
		}
	}
	return out
}

// computeRequestBounds returns the minimum Key and maximum EndKey
// across the given spans, suitable for a GetTxnDetailsRequest header.
func computeRequestBounds(spans []roachpb.Span) (roachpb.Key, roachpb.Key) {
	minKey := spans[0].Key
	maxEndKey := spans[0].EndKey
	if len(maxEndKey) == 0 {
		maxEndKey = spans[0].Key.Next()
	}
	for _, s := range spans[1:] {
		if s.Key.Compare(minKey) < 0 {
			minKey = s.Key
		}
		ek := s.EndKey
		if len(ek) == 0 {
			ek = s.Key.Next()
		}
		if ek.Compare(maxEndKey) > 0 {
			maxEndKey = ek
		}
	}
	return minKey, maxEndKey
}

func logTxnFeedMessage(ctx context.Context, tag string, msg *kvpb.TxnFeedMessage) {
	if c := msg.Event.Committed; c != nil {
		hasDetails := msg.Details != nil
		numWrites := 0
		numDeps := 0
		if msg.Details != nil {
			numWrites = len(msg.Details.Writes)
			numDeps = len(msg.Details.Dependencies)
		}
		log.Dev.Infof(ctx,
			"%s: COMMITTED txn=%s ts=%s anchor=%s writeSpans=%v readSpans=%v "+
				"hasDetails=%v numWrites=%d numDeps=%d",
			tag, c.TxnID.Short(), c.CommitTimestamp, c.AnchorKey,
			c.WriteSpans, c.ReadSpans,
			hasDetails, numWrites, numDeps)
	} else if cp := msg.Event.Checkpoint; cp != nil {
		log.Dev.Infof(ctx, "%s: CHECKPOINT span=%s resolved=%s",
			tag, cp.AnchorSpan, cp.ResolvedTS)
	} else if e := msg.Event.Error; e != nil {
		log.Dev.Infof(ctx, "%s: ERROR %v", tag, e.Error)
	}
}
