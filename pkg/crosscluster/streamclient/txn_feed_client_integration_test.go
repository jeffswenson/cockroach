// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// remoteTxnFeedTestCluster wraps setup for RemoteTxnFeedClient
// integration tests. Unlike the DistSender-level tests in kvcoord
// that use raw KV operations, these tests exercise the full
// txnproducer→txnsubscriber path with SQL workloads.
type remoteTxnFeedTestCluster struct {
	tc       *testcluster.TestCluster
	sqlDB    *sql.DB
	client   *streamclient.RemoteTxnFeedClient
	codec    keys.SQLCodec
	feedSpan roachpb.Span
}

// setupRemoteTxnFeedTest creates a test cluster, a SQL table, a
// replication stream, and a RemoteTxnFeedClient wired to that stream.
// The returned feedSpan covers the test table's primary index.
func setupRemoteTxnFeedTest(
	t *testing.T, ctx context.Context,
) (c *remoteTxnFeedTestCluster, stop func()) {
	t.Helper()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)
	kvserver.RangefeedEnabled.Override(ctx, &settings.SV, true)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})

	sqlDB := tc.ServerConn(0)
	_, err := sqlDB.Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	_, err = sqlDB.Exec(
		"CREATE TABLE defaultdb.feed_test (k INT PRIMARY KEY, v TEXT)")
	require.NoError(t, err)

	// Create a replication stream for the test table.
	req := streampb.ReplicationProducerRequest{
		TableNames: []string{"defaultdb.public.feed_test"},
	}
	reqBytes, err := protoutil.Marshal(&req)
	require.NoError(t, err)

	var specBytes []byte
	err = sqlDB.QueryRow(
		"SELECT crdb_internal.start_replication_stream_for_tables($1)",
		reqBytes,
	).Scan(&specBytes)
	require.NoError(t, err)

	var spec streampb.ReplicationProducerSpec
	require.NoError(t, protoutil.Unmarshal(specBytes, &spec))

	// Build pgx config for the RemoteTxnFeedClient.
	pgURL, cleanupCerts := pgurlutils.PGUrl(
		t, tc.Server(0).SystemLayer().AdvSQLAddr(),
		t.Name(), url.User(username.RootUser))

	pgxConfig, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)

	client := streamclient.NewRemoteTxnFeedClient(pgxConfig, spec.StreamID)

	// Get the table's KV span.
	codec := tc.Server(0).ApplicationLayer().Codec()
	desc := desctestutils.TestingGetPublicTableDescriptor(
		tc.Server(0).DB(), codec, "defaultdb", "feed_test")
	feedSpan := desc.TableSpan(codec)

	return &remoteTxnFeedTestCluster{
			tc:       tc,
			sqlDB:    sqlDB,
			client:   client,
			codec:    codec,
			feedSpan: feedSpan,
		}, func() {
			cleanupCerts()
			tc.Stopper().Stop(ctx)
		}
}

// startRemoteFeedAndDrain starts a TxnFeed and continuously drains
// events into a slice. Returns a getter for collected events, a cancel
// function, and an error channel.
func startRemoteFeedAndDrain(
	t *testing.T,
	ctx context.Context,
	client *streamclient.RemoteTxnFeedClient,
	span roachpb.Span,
	startAfter hlc.Timestamp,
	opts ...kvcoord.TxnFeedOption,
) (getEvents func() []kvpb.TxnFeedMessage, cancel func(), feedErrCh <-chan error) {
	t.Helper()
	eventCh := make(chan kvpb.TxnFeedMessage, 1024)
	errCh := make(chan error, 1)
	feedCtx, feedCancel := context.WithCancel(ctx)

	go func() {
		errCh <- client.TxnFeed(feedCtx, []kvcoord.SpanTimePair{
			{Span: span, StartAfter: startAfter},
		}, eventCh, opts...)
	}()

	var (
		mu     sync.Mutex
		events []kvpb.TxnFeedMessage
	)
	go func() {
		for {
			select {
			case msg := <-eventCh:
				mu.Lock()
				events = append(events, msg)
				mu.Unlock()
			case <-feedCtx.Done():
				return
			}
		}
	}()

	return func() []kvpb.TxnFeedMessage {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]kvpb.TxnFeedMessage, len(events))
		copy(cp, events)
		return cp
	}, feedCancel, errCh
}

// TestRemoteTxnFeedClient_Live starts a feed, runs a SQL workload,
// and verifies that committed transaction events appear in the feed.
func TestRemoteTxnFeedClient_Live(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	c, stop := setupRemoteTxnFeedTest(t, ctx)
	defer stop()

	startTS := c.tc.Server(0).Clock().Now()
	getEvents, cancelFeed, feedErrCh := startRemoteFeedAndDrain(
		t, ctx, c.client, c.feedSpan, startTS)

	// Run a SQL workload.
	const numRows = 20
	for i := 0; i < numRows; i++ {
		_, err := c.sqlDB.Exec(
			"UPSERT INTO defaultdb.feed_test (k, v) VALUES ($1, $2)",
			i, fmt.Sprintf("val-%d", i))
		require.NoError(t, err)
	}

	// Wait for events to arrive. We expect at least one committed
	// event per row written.
	testutils.SucceedsSoon(t, func() error {
		events := getEvents()
		var committed int
		for _, ev := range events {
			if ev.Event.Committed != nil {
				committed++
			}
		}
		if committed < numRows {
			return fmt.Errorf(
				"waiting for %d committed events, got %d", numRows, committed)
		}
		return nil
	})

	cancelFeed()
	select {
	case err := <-feedErrCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for feed to stop")
	}
}

// TestRemoteTxnFeedClient_Enriched starts a feed with enrichment
// enabled, runs a SQL workload, and verifies that committed events
// carry write details.
func TestRemoteTxnFeedClient_Enriched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	c, stop := setupRemoteTxnFeedTest(t, ctx)
	defer stop()

	startTS := c.tc.Server(0).Clock().Now()
	getEvents, cancelFeed, feedErrCh := startRemoteFeedAndDrain(
		t, ctx, c.client, c.feedSpan, startTS,
		kvcoord.WithEnrichment(),
	)

	// Run a SQL workload.
	const numRows = 10
	for i := 0; i < numRows; i++ {
		_, err := c.sqlDB.Exec(
			"UPSERT INTO defaultdb.feed_test (k, v) VALUES ($1, $2)",
			i, fmt.Sprintf("enriched-%d", i))
		require.NoError(t, err)
	}

	// Wait for committed events with details.
	testutils.SucceedsSoon(t, func() error {
		events := getEvents()
		var committed int
		for _, ev := range events {
			if ev.Event.Committed != nil {
				committed++
			}
		}
		if committed < numRows {
			return fmt.Errorf(
				"waiting for %d committed events, got %d", numRows, committed)
		}
		return nil
	})

	// Verify that all committed events have non-nil Details with
	// at least one write.
	events := getEvents()
	for _, ev := range events {
		if ev.Event.Committed == nil {
			continue
		}
		require.NotNilf(t, ev.Details,
			"committed txn %s missing Details", ev.Event.Committed.TxnID.Short())
		require.NotEmptyf(t, ev.Details.Writes,
			"committed txn %s has empty Writes", ev.Event.Committed.TxnID.Short())
	}

	cancelFeed()
	select {
	case err := <-feedErrCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for feed to stop")
	}
}
