// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
)

// TxnFeedClient abstracts a TxnFeed data source, allowing both the
// local DistSender path and the remote txnproducer→txnsubscriber path
// to be used interchangeably.
type TxnFeedClient interface {
	// TxnFeed starts streaming TxnFeed events for the given spans.
	// Events are delivered on eventCh. Blocks until the context is
	// cancelled or a fatal error occurs.
	TxnFeed(
		ctx context.Context,
		spans []kvcoord.SpanTimePair,
		eventCh chan<- kvpb.TxnFeedMessage,
		opts ...kvcoord.TxnFeedOption,
	) error
}

// RemoteTxnFeedClient implements TxnFeedClient by calling the
// crdb_internal.txn_feed_partition SQL builtin on a remote source
// cluster over a pgx connection.
type RemoteTxnFeedClient struct {
	pgxConfig *pgx.ConnConfig
	streamID  streampb.StreamID
}

// NewRemoteTxnFeedClient creates a RemoteTxnFeedClient that connects to
// the source cluster described by pgxConfig.
func NewRemoteTxnFeedClient(
	pgxConfig *pgx.ConnConfig, streamID streampb.StreamID,
) *RemoteTxnFeedClient {
	return &RemoteTxnFeedClient{
		pgxConfig: pgxConfig,
		streamID:  streamID,
	}
}

// TxnFeed implements TxnFeedClient.
func (r *RemoteTxnFeedClient) TxnFeed(
	ctx context.Context,
	spans []kvcoord.SpanTimePair,
	eventCh chan<- kvpb.TxnFeedMessage,
	opts ...kvcoord.TxnFeedOption,
) error {
	spec := buildTxnFeedPartitionSpec(spans, opts)
	specBytes, err := protoutil.Marshal(&spec)
	if err != nil {
		return errors.Wrap(err, "marshaling TxnFeedPartitionSpec")
	}

	srcConn, err := pgx.ConnectConfig(ctx, r.pgxConfig)
	if err != nil {
		return errors.Wrap(err, "connecting to source cluster")
	}
	defer func() { _ = srcConn.Close(ctx) }()

	if _, err := srcConn.Exec(ctx,
		"SET avoid_buffering = true"); err != nil {
		return err
	}
	if _, err := srcConn.Exec(ctx,
		"SET statement_timeout = '0s'"); err != nil {
		return err
	}

	rows, err := srcConn.Query(ctx,
		`SELECT * FROM crdb_internal.txn_feed_partition($1, $2)`,
		r.streamID, specBytes)
	if err != nil {
		return errors.Wrap(err, "starting txn feed partition query")
	}
	defer rows.Close()

	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return errors.Wrap(err, "scanning txn feed row")
		}
		var batch streampb.TxnFeedBatch
		if err := protoutil.Unmarshal(data, &batch); err != nil {
			return errors.Wrap(err, "unmarshaling TxnFeedBatch")
		}
		for i := range batch.Messages {
			select {
			case eventCh <- batch.Messages[i]:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return rows.Err()
}

// buildTxnFeedPartitionSpec converts SpanTimePairs and TxnFeedOptions
// into a TxnFeedPartitionSpec for wire transmission.
func buildTxnFeedPartitionSpec(
	spans []kvcoord.SpanTimePair, opts []kvcoord.TxnFeedOption,
) streampb.TxnFeedPartitionSpec {
	spec := streampb.TxnFeedPartitionSpec{
		Spans: make(
			[]jobspb.ResolvedSpan, len(spans)),
	}
	for i, sp := range spans {
		spec.Spans[i] = jobspb.ResolvedSpan{
			Span:      sp.Span,
			Timestamp: sp.StartAfter,
		}
	}

	// Apply options to extract enrichment configuration.
	cfg := kvcoord.ApplyTxnFeedOptions(opts)
	spec.Enrich = cfg.Enrich
	spec.DetailSpans = cfg.DetailSpans
	spec.DepOnlySpans = cfg.DepOnlySpans

	return spec
}
