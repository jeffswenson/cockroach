// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
)

// TxnFeedOption configures the behavior of DistSender.TxnFeed.
type TxnFeedOption interface {
	apply(*txnFeedConfig)
}

type txnFeedConfig struct {
	enrich       bool
	detailSpans  []roachpb.Span
	depOnlySpans []roachpb.Span
}

// TxnFeedConfig holds the resolved configuration from a set of
// TxnFeedOption. It is used by the remote subscriber to serialize
// options into a TxnFeedPartitionSpec.
type TxnFeedConfig struct {
	Enrich       bool
	DetailSpans  []roachpb.Span
	DepOnlySpans []roachpb.Span
}

// ApplyTxnFeedOptions applies the given options and returns the
// resolved configuration.
func ApplyTxnFeedOptions(opts []TxnFeedOption) TxnFeedConfig {
	var cfg txnFeedConfig
	for _, o := range opts {
		o.apply(&cfg)
	}
	return TxnFeedConfig{
		Enrich:       cfg.enrich,
		DetailSpans:  cfg.detailSpans,
		DepOnlySpans: cfg.depOnlySpans,
	}
}

type txnFeedOptionFunc func(*txnFeedConfig)

func (f txnFeedOptionFunc) apply(c *txnFeedConfig) { f(c) }

func (c *txnFeedConfig) validate() error {
	if len(c.detailSpans) > 0 && len(c.depOnlySpans) > 0 {
		for _, dep := range c.depOnlySpans {
			overlaps := false
			for _, det := range c.detailSpans {
				if dep.Overlaps(det) {
					overlaps = true
					break
				}
			}
			if !overlaps {
				return errors.Newf(
					"dependency-only span %s does not overlap with any detail span", dep)
			}
		}
	}
	return nil
}

// WithEnrichment enables automatic GetTxnDetails enrichment. When
// enabled, each committed event is enriched with full write data and
// dependency information before being delivered on the event channel.
func WithEnrichment() TxnFeedOption {
	return txnFeedOptionFunc(func(c *txnFeedConfig) {
		c.enrich = true
	})
}

// WithDetailSpans restricts which read and write spans are sent in
// GetTxnDetails requests. Only spans from the committed event that
// overlap with at least one of the given spans are included. When no
// detail spans are set (or the slice is empty), all of the
// transaction's spans are sent unfiltered.
func WithDetailSpans(spans []roachpb.Span) TxnFeedOption {
	return txnFeedOptionFunc(func(c *txnFeedConfig) {
		c.detailSpans = spans
	})
}

// WithDependencyOnlySpans specifies spans for which write data is not
// needed but dependency information is. Write spans from the committed
// event that are fully contained by a dependency-only span are moved
// from the write set to the read set in the GetTxnDetails request.
// The server will compute dependencies for those spans but will not
// fetch the actual write KVs.
//
// Write spans that only partially overlap a dependency-only span are
// kept as writes, since the non-overlapping portion may contain data
// the caller needs.
//
// When detail spans are configured, every dependency-only span must
// overlap with at least one detail span.
func WithDependencyOnlySpans(spans []roachpb.Span) TxnFeedOption {
	return txnFeedOptionFunc(func(c *txnFeedConfig) {
		c.depOnlySpans = spans
	})
}

// TxnFeed divides TxnFeed requests across range boundaries and
// establishes multiplexed TxnFeed streams. Events are delivered on
// eventCh. Blocks until the context is cancelled or a fatal error
// occurs.
func (ds *DistSender) TxnFeed(
	ctx context.Context,
	spans []SpanTimePair,
	eventCh chan<- kvpb.TxnFeedMessage,
	opts ...TxnFeedOption,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}
	var cfg txnFeedConfig
	for _, o := range opts {
		o.apply(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if !cfg.enrich {
		return muxTxnFeed(ctx, spans, ds, eventCh)
	}
	// Buffer rawCh so the mux layer can continue producing events
	// while the enricher is blocked on GetTxnDetails RPCs.
	rawCh := make(chan kvpb.TxnFeedMessage, 4096)
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(rawCh)
		return muxTxnFeed(ctx, spans, ds, rawCh)
	})
	g.GoCtx(func(ctx context.Context) error {
		return runTxnFeedEnricher(ctx, ds, spans, cfg.detailSpans, cfg.depOnlySpans, rawCh, eventCh)
	})
	return g.Wait()
}

// txnFeedErrorInfo describes how a txnfeed error should be handled.
type txnFeedErrorInfo struct {
	evict       bool // Evict routing info before retry.
	resolveSpan bool // Re-divide span on range boundaries.
}

// handleTxnFeedError classifies a txnfeed error and returns a
// recovery strategy. Returns a non-nil error if the txnfeed should
// terminate.
func handleTxnFeedError(ctx context.Context, err error) (txnFeedErrorInfo, error) {
	if err == nil {
		return txnFeedErrorInfo{}, nil
	}
	switch {
	case errors.Is(err, io.EOF):
		return txnFeedErrorInfo{}, nil
	case errors.HasType(err, (*kvpb.StoreNotFoundError)(nil)):
		return txnFeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.NodeUnavailableError)(nil)):
		return txnFeedErrorInfo{}, nil
	case IsSendError(err):
		return txnFeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)):
		return txnFeedErrorInfo{evict: true}, nil
	case errors.HasType(err, (*kvpb.RangeKeyMismatchError)(nil)):
		return txnFeedErrorInfo{evict: true, resolveSpan: true}, nil
	case errors.HasType(err, (*kvpb.TxnFeedRetryError)(nil)):
		var t *kvpb.TxnFeedRetryError
		if ok := errors.As(err, &t); !ok {
			return txnFeedErrorInfo{}, errors.AssertionFailedf(
				"wrong error type: %T", err)
		}
		switch t.Reason {
		case kvpb.TxnFeedRetryError_REASON_REPLICA_REMOVED,
			kvpb.TxnFeedRetryError_REASON_RAFT_SNAPSHOT,
			kvpb.TxnFeedRetryError_REASON_TXNFEED_CLOSED:
			return txnFeedErrorInfo{}, nil
		case kvpb.TxnFeedRetryError_REASON_RANGE_SPLIT,
			kvpb.TxnFeedRetryError_REASON_RANGE_MERGED:
			return txnFeedErrorInfo{evict: true, resolveSpan: true}, nil
		case kvpb.TxnFeedRetryError_REASON_MANUAL_RANGE_SPLIT:
			return txnFeedErrorInfo{evict: true, resolveSpan: true}, nil
		default:
			return txnFeedErrorInfo{}, errors.AssertionFailedf(
				"unrecognized TxnFeedRetryError reason: %v", t.Reason)
		}
	default:
		return txnFeedErrorInfo{}, err
	}
}
