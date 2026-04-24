// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// TestBuildTxnFeedPartitionSpec verifies that every TxnFeedOption is
// correctly mapped into the TxnFeedPartitionSpec wire format. If a
// new option is added to TxnFeedConfig but not wired through
// buildTxnFeedPartitionSpec, this test should fail.
func TestBuildTxnFeedPartitionSpec(t *testing.T) {
	spans := []kvcoord.SpanTimePair{
		{
			Span:       roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			StartAfter: hlc.Timestamp{WallTime: 100},
		},
		{
			Span:       roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
			StartAfter: hlc.Timestamp{WallTime: 200},
		},
	}

	detailSpans := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
	}
	depOnlySpans := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("a1")},
	}

	opts := []kvcoord.TxnFeedOption{
		kvcoord.WithEnrichment(),
		kvcoord.WithDetailSpans(detailSpans),
		kvcoord.WithDependencyOnlySpans(depOnlySpans),
	}

	spec := buildTxnFeedPartitionSpec(spans, opts)

	// Verify spans.
	require.Len(t, spec.Spans, 2)
	require.Equal(t, roachpb.Key("a"), spec.Spans[0].Span.Key)
	require.Equal(t, roachpb.Key("b"), spec.Spans[0].Span.EndKey)
	require.Equal(t, hlc.Timestamp{WallTime: 100}, spec.Spans[0].Timestamp)
	require.Equal(t, roachpb.Key("c"), spec.Spans[1].Span.Key)
	require.Equal(t, hlc.Timestamp{WallTime: 200}, spec.Spans[1].Timestamp)

	// Verify options.
	require.True(t, spec.Enrich)
	require.Equal(t, detailSpans, spec.DetailSpans)
	require.Equal(t, depOnlySpans, spec.DepOnlySpans)

	// Verify that ApplyTxnFeedOptions and buildTxnFeedPartitionSpec
	// agree on the set of fields. If a new field is added to
	// TxnFeedConfig but not mapped in buildTxnFeedPartitionSpec, this
	// round-trip will expose the gap.
	cfg := kvcoord.ApplyTxnFeedOptions(opts)
	require.Equal(t, cfg.Enrich, spec.Enrich)
	require.Equal(t, cfg.DetailSpans, spec.DetailSpans)
	require.Equal(t, cfg.DepOnlySpans, spec.DepOnlySpans)
}
