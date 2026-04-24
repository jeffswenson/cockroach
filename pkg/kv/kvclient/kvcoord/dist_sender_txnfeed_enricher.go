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
		if err := enrichBatch(ctx, ds, frontier, batch, detailSpans, depOnlySpans); err != nil {
			return err
		}

		// Forward all events to consumer in order.
		for i := range batch {
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
		readSpans := filterOverlappingSpans(c.ReadSpans, detailSpans)
		writeSpans := filterOverlappingSpans(c.WriteSpans, detailSpans)
		readSpans, writeSpans = moveDepOnlyWrites(readSpans, writeSpans, depOnlySpans)
		allSpans := slices.Concat(writeSpans, readSpans)

		var minKey, maxEndKey roachpb.Key
		if len(allSpans) > 0 {
			minKey, maxEndKey = computeRequestBounds(allSpans)
		} else {
			// No spans overlap with detailSpans; send a point
			// request at the anchor key so we still get a response.
			minKey = c.AnchorKey
			maxEndKey = c.AnchorKey.Next()
		}

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
