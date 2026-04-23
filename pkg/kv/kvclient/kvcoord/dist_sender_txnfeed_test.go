// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// txnFeedTestCluster wraps common setup for txnfeed tests.
type txnFeedTestCluster struct {
	tc         *testcluster.TestCluster
	db         *kv.DB
	ds         *kvcoord.DistSender
	scratchKey roachpb.Key
}

// setupTxnFeedTest creates a test cluster with txnfeed enabled.
// The caller must defer stop() to shut down the cluster — this must
// be deferred AFTER leaktest.AfterTest so the cluster stops before
// the goroutine leak check runs.
func setupTxnFeedTest(
	t *testing.T, ctx context.Context, numNodes int,
) (c *txnFeedTestCluster, stop func()) {
	t.Helper()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})

	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	return &txnFeedTestCluster{
		tc:         tc,
		db:         tc.Server(0).DB(),
		ds:         tc.Server(0).DistSenderI().(*kvcoord.DistSender),
		scratchKey: tc.ScratchRange(t),
	}, func() { tc.Stopper().Stop(ctx) }
}

type txnOp struct {
	key   roachpb.Key
	value string // empty for reads
	isGet bool   // true for reads, false for writes
}

type committedTxn struct {
	id    uuid.UUID
	label string
	ops   []txnOp
}

// runWorkload runs concurrent random transactions for the given
// duration. Returns the list of committed transaction IDs.
func runWorkload(
	t *testing.T,
	ctx context.Context,
	db *kv.DB,
	scratchKey roachpb.Key,
	numWorkers int,
	dur time.Duration,
) []committedTxn {
	t.Helper()
	const keyAlphabet = "abcdefghijklmnopqrstuvwxyz"

	var (
		mu   sync.Mutex
		txns []committedTxn
	)

	randomKey := func(rng *rand.Rand) roachpb.Key {
		k := scratchKey.Clone()
		k = append(k, keyAlphabet[rng.Intn(len(keyAlphabet))])
		k = append(k, byte('0'+rng.Intn(10)))
		return k
	}

	var wg sync.WaitGroup
	workloadCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))
			for i := 0; workloadCtx.Err() == nil; i++ {
				label := fmt.Sprintf("w%d-t%d", workerID, i)
				switch rng.Intn(4) {
				case 0:
					// Single-key write.
					key := randomKey(rng)
					var id uuid.UUID
					err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						id = txn.ID()
						return txn.Put(ctx, key, label)
					})
					if err != nil {
						continue
					}
					mu.Lock()
					txns = append(txns, committedTxn{
						id: id, label: label,
						ops: []txnOp{{key: key, value: label}},
					})
					mu.Unlock()
				case 1:
					// Two-key write.
					key1 := randomKey(rng)
					key2 := randomKey(rng)
					for key1.Equal(key2) {
						key2 = randomKey(rng)
					}
					txn := kv.NewTxn(ctx, db, 0)
					id := txn.ID()
					if err := txn.Put(ctx, key1, label+"-a"); err != nil {
						continue
					}
					if err := txn.Put(ctx, key2, label+"-b"); err != nil {
						continue
					}
					if err := txn.Commit(ctx); err != nil {
						continue
					}
					mu.Lock()
					txns = append(txns, committedTxn{
						id: id, label: label,
						ops: []txnOp{
							{key: key1, value: label + "-a"},
							{key: key2, value: label + "-b"},
						},
					})
					mu.Unlock()
				case 2:
					// Read + write.
					readKey := randomKey(rng)
					writeKey := randomKey(rng)
					for readKey.Equal(writeKey) {
						writeKey = randomKey(rng)
					}
					var id uuid.UUID
					err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						id = txn.ID()
						if _, err := txn.Get(ctx, readKey); err != nil {
							return err
						}
						return txn.Put(ctx, writeKey, label)
					})
					if err != nil {
						continue
					}
					mu.Lock()
					txns = append(txns, committedTxn{
						id: id, label: label,
						ops: []txnOp{
							{key: readKey, isGet: true},
							{key: writeKey, value: label},
						},
					})
					mu.Unlock()
				case 3:
					// Read-only.
					readKey := randomKey(rng)
					var id uuid.UUID
					err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						id = txn.ID()
						_, err := txn.Get(ctx, readKey)
						return err
					})
					if err != nil {
						continue
					}
					mu.Lock()
					txns = append(txns, committedTxn{
						id: id, label: label,
						ops: []txnOp{{key: readKey, isGet: true}},
					})
					mu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()
	return txns
}

// writingTxns filters to transactions that performed at least one
// write. Read-only transactions don't appear on the txn feed.
func writingTxns(txns []committedTxn) []committedTxn {
	var out []committedTxn
	for _, ct := range txns {
		for _, op := range ct.ops {
			if !op.isGet {
				out = append(out, ct)
				break
			}
		}
	}
	return out
}

// startFeedAndDrain starts a TxnFeed and continuously drains events.
// Returns functions to get collected events and to cancel the feed.
func startFeedAndDrain(
	t *testing.T,
	ctx context.Context,
	ds *kvcoord.DistSender,
	span roachpb.Span,
	startAfter hlc.Timestamp,
	opts ...kvcoord.TxnFeedOption,
) (getEvents func() []kvcoord.TxnFeedMessage, cancel func(), feedErrCh <-chan error) {
	t.Helper()
	eventCh := make(chan kvcoord.TxnFeedMessage, 1024)
	errCh := make(chan error, 1)
	feedCtx, feedCancel := context.WithCancel(ctx)

	go func() {
		errCh <- ds.TxnFeed(feedCtx, []kvcoord.SpanTimePair{
			{Span: span, StartAfter: startAfter},
		}, eventCh, opts...)
	}()

	var (
		mu     sync.Mutex
		events []kvcoord.TxnFeedMessage
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

	return func() []kvcoord.TxnFeedMessage {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]kvcoord.TxnFeedMessage, len(events))
		copy(cp, events)
		return cp
	}, feedCancel, errCh
}

// waitForTxns waits until all expected transactions appear in the feed.
func waitForTxns(
	t *testing.T,
	txns []committedTxn,
	getEvents func() []kvcoord.TxnFeedMessage,
	feedErrCh <-chan error,
) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		select {
		case feedErr := <-feedErrCh:
			t.Fatalf("TxnFeed exited with error: %v", feedErr)
		default:
		}
		evs := getEvents()
		seen := make(map[uuid.UUID]struct{})
		for _, ev := range evs {
			if ev.Committed != nil {
				seen[ev.Committed.TxnID] = struct{}{}
			}
		}
		missing := 0
		for _, ct := range txns {
			if _, ok := seen[ct.id]; !ok {
				missing++
			}
		}
		if missing == 0 {
			return nil
		}
		return fmt.Errorf("waiting for %d/%d committed txns (%d events so far)",
			missing, len(txns), len(evs))
	})
}

// validateEvents checks completeness, checkpoint ordering, and event
// validity on the collected events.
func validateEvents(
	t *testing.T,
	txns []committedTxn,
	collectedEvents []kvcoord.TxnFeedMessage,
	feedSpan roachpb.Span,
) {
	t.Helper()

	wantIDs := make(map[uuid.UUID]string)
	for _, ct := range txns {
		wantIDs[ct.id] = ct.label
	}

	committedEvents := make(map[uuid.UUID]*kvpb.TxnFeedCommitted)
	for _, ev := range collectedEvents {
		if ev.Committed != nil {
			committedEvents[ev.Committed.TxnID] = ev.Committed
		}
	}

	// 1. Completeness.
	for id, label := range wantIDs {
		_, ok := committedEvents[id]
		require.True(t, ok, "txn %s (%s) missing from feed", id.Short(), label)
	}
	t.Logf("completeness: all %d txns present", len(wantIDs))

	// 2. Checkpoint ordering: a committed event must not appear
	//    after a checkpoint that covers its anchor span.
	maxResolved := make(map[string]hlc.Timestamp)
	spanByKey := make(map[string]roachpb.Span)
	for _, ev := range collectedEvents {
		if ev.Checkpoint != nil {
			key := ev.Checkpoint.AnchorSpan.String()
			if ev.Checkpoint.ResolvedTS.Less(maxResolved[key]) {
				t.Errorf("checkpoint went backwards for %s: %s < %s",
					key, ev.Checkpoint.ResolvedTS, maxResolved[key])
			}
			maxResolved[key] = ev.Checkpoint.ResolvedTS
			spanByKey[key] = ev.Checkpoint.AnchorSpan
		}
		if ev.Committed != nil {
			anchorKey, err := keys.Addr(ev.Committed.AnchorKey)
			if err != nil {
				continue
			}
			for spanKey, resolved := range maxResolved {
				if !spanByKey[spanKey].ContainsKey(anchorKey.AsRawKey()) {
					continue
				}
				if ev.Committed.CommitTimestamp.Less(resolved) {
					t.Errorf("committed txn %s at %s after checkpoint %s for %s",
						ev.Committed.TxnID.Short(),
						ev.Committed.CommitTimestamp, resolved, spanKey)
				}
			}
		}
	}
	t.Logf("checkpoint ordering: validated across %d events", len(collectedEvents))

	// 3. Event validity.
	for _, c := range committedEvents {
		require.NotEqual(t, uuid.UUID{}, c.TxnID, "zero TxnID")
		require.False(t, c.CommitTimestamp.IsEmpty(), "txn %s: zero commit ts", c.TxnID.Short())
		addrKey, err := keys.Addr(c.AnchorKey)
		require.NoError(t, err, "txn %s: invalid anchor key", c.TxnID.Short())
		require.True(t, feedSpan.ContainsKey(addrKey.AsRawKey()),
			"anchor %s (addr %s) outside %s", c.AnchorKey, addrKey, feedSpan)
	}
	t.Logf("event validity: all %d committed events valid", len(committedEvents))
}

// validateEnrichment checks that enriched events carry correct write
// data matching the ground truth from the workload.
func validateEnrichment(
	t *testing.T,
	txns []committedTxn,
	collectedEvents []kvcoord.TxnFeedMessage,
	feedSpan roachpb.Span,
) {
	t.Helper()

	// Build ground truth: txnID → expected writes.
	type expectedWrite struct {
		key   roachpb.Key
		value string
	}
	wantWrites := make(map[uuid.UUID][]expectedWrite)
	for _, ct := range txns {
		var writes []expectedWrite
		for _, op := range ct.ops {
			if !op.isGet {
				writes = append(writes, expectedWrite{key: op.key, value: op.value})
			}
		}
		wantWrites[ct.id] = writes
	}

	// 1. Every committed event must have Details.
	for _, ev := range collectedEvents {
		if ev.Committed == nil {
			require.Nilf(t, ev.Details,
				"non-committed event has Details set")
			continue
		}
		require.NotNilf(t, ev.Details,
			"committed txn %s missing Details", ev.Committed.TxnID.Short())
	}

	// 2. For workload txns, verify write data matches ground truth.
	for _, ev := range collectedEvents {
		if ev.Committed == nil {
			continue
		}
		expected, isOurs := wantWrites[ev.Committed.TxnID]
		if !isOurs {
			continue
		}

		// Build a map of actual writes by key.
		gotWrites := make(map[string]string)
		for _, w := range ev.Details.Writes {
			val, err := w.KeyValue.Value.GetBytes()
			if err != nil {
				// Tombstone — value is empty.
				gotWrites[string(w.KeyValue.Key)] = ""
			} else {
				gotWrites[string(w.KeyValue.Key)] = string(val)
			}
		}

		// Every expected write must appear with the correct value.
		for _, ew := range expected {
			gotVal, ok := gotWrites[string(ew.key)]
			require.Truef(t, ok,
				"txn %s: expected write at %s not found in Details.Writes",
				ev.Committed.TxnID.Short(), ew.key)
			require.Equalf(t, ew.value, gotVal,
				"txn %s: wrong value at %s", ev.Committed.TxnID.Short(), ew.key)
		}

		// Verify write count matches (no unexpected extra writes).
		require.Equalf(t, len(expected), len(ev.Details.Writes),
			"txn %s: unexpected write count", ev.Committed.TxnID.Short())
	}

	// 3. All write keys in Details must be within the feed span.
	for _, ev := range collectedEvents {
		if ev.Committed == nil || ev.Details == nil {
			continue
		}
		for _, w := range ev.Details.Writes {
			require.Truef(t, feedSpan.ContainsKey(w.KeyValue.Key),
				"txn %s: write key %s outside feed span %s",
				ev.Committed.TxnID.Short(), w.KeyValue.Key, feedSpan)
		}
	}

	// 4. Checkpoint ordering still holds with enrichment (enriched
	//    committed events must not appear after a checkpoint that
	//    covers their anchor span).
	maxResolved := make(map[string]hlc.Timestamp)
	spanByKey := make(map[string]roachpb.Span)
	for _, ev := range collectedEvents {
		if ev.Checkpoint != nil {
			key := ev.Checkpoint.AnchorSpan.String()
			maxResolved[key] = ev.Checkpoint.ResolvedTS
			spanByKey[key] = ev.Checkpoint.AnchorSpan
		}
		if ev.Committed != nil {
			anchorKey, err := keys.Addr(ev.Committed.AnchorKey)
			if err != nil {
				continue
			}
			for spanKey, resolved := range maxResolved {
				if !spanByKey[spanKey].ContainsKey(anchorKey.AsRawKey()) {
					continue
				}
				if ev.Committed.CommitTimestamp.Less(resolved) {
					t.Errorf("enriched committed txn %s at %s after checkpoint %s for %s",
						ev.Committed.TxnID.Short(),
						ev.Committed.CommitTimestamp, resolved, spanKey)
				}
			}
		}
	}

	t.Logf("enrichment validation: passed for %d workload txns", len(txns))
}

// TestDistSenderTxnFeed_Live runs a concurrent workload with a
// mid-workload range split and validates the live event stream.
func TestDistSenderTxnFeed_Live(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	c, stop := setupTxnFeedTest(t, ctx, 1)
	defer stop()
	for _, split := range []byte{'c', 'f', 'i'} {
		c.tc.SplitRangeOrFatal(t, append(c.scratchKey.Clone(), split))
	}

	feedSpan := roachpb.Span{Key: c.scratchKey, EndKey: c.scratchKey.PrefixEnd()}
	startTS := c.db.Clock().Now()

	getEvents, cancelFeed, feedErrCh := startFeedAndDrain(t, ctx, c.ds, feedSpan, startTS)

	// Run workload with a mid-workload split.
	var splitWg sync.WaitGroup
	splitWg.Add(1)
	go func() {
		defer splitWg.Done()
		time.Sleep(time.Second)
		splitKey := append(c.scratchKey.Clone(), 'd', '5')
		t.Logf("splitting range at %s mid-workload", splitKey)
		c.tc.SplitRangeOrFatal(t, splitKey)
	}()

	allTxns := runWorkload(t, ctx, c.db, c.scratchKey, 8, 3*time.Second)
	txns := writingTxns(allTxns)
	splitWg.Wait()
	t.Logf("workload complete: %d txns total, %d with writes", len(allTxns), len(txns))

	waitForTxns(t, txns, getEvents, feedErrCh)
	cancelFeed()
	validateEvents(t, txns, getEvents(), feedSpan)
}

// TestDistSenderTxnFeed_CatchUpScan commits transactions before
// starting the feed with an earlier timestamp, verifying that the
// server's catch-up scan delivers historical events.
func TestDistSenderTxnFeed_CatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	c, stop := setupTxnFeedTest(t, ctx, 1)
	defer stop()
	for _, split := range []byte{'c', 'f', 'i'} {
		c.tc.SplitRangeOrFatal(t, append(c.scratchKey.Clone(), split))
	}

	feedSpan := roachpb.Span{Key: c.scratchKey, EndKey: c.scratchKey.PrefixEnd()}

	// Record a timestamp BEFORE any transactions.
	cursorTS := c.db.Clock().Now()

	// Run a workload — these transactions will be in the past
	// relative to when we start the feed.
	allTxns := runWorkload(t, ctx, c.db, c.scratchKey, 4, 1*time.Second)
	txns := writingTxns(allTxns)
	t.Logf("pre-feed workload: %d txns total, %d with writes", len(allTxns), len(txns))

	// Now start the feed at the old cursor. The server must deliver
	// all committed transactions via catch-up scan.
	getEvents, cancelFeed, feedErrCh := startFeedAndDrain(t, ctx, c.ds, feedSpan, cursorTS)

	waitForTxns(t, txns, getEvents, feedErrCh)
	cancelFeed()
	validateEvents(t, txns, getEvents(), feedSpan)
}

// TestDistSenderTxnFeed_Enriched runs a concurrent workload with
// enrichment enabled and verifies that committed events carry full
// write data from GetTxnDetails.
func TestDistSenderTxnFeed_Enriched(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	c, stop := setupTxnFeedTest(t, ctx, 1)
	defer stop()
	for _, split := range []byte{'c', 'f', 'i'} {
		c.tc.SplitRangeOrFatal(t, append(c.scratchKey.Clone(), split))
	}

	feedSpan := roachpb.Span{
		Key: c.scratchKey, EndKey: c.scratchKey.PrefixEnd(),
	}
	startTS := c.db.Clock().Now()

	getEvents, cancelFeed, feedErrCh := startFeedAndDrain(
		t, ctx, c.ds, feedSpan, startTS, kvcoord.WithEnrichment(),
	)

	allTxns := runWorkload(t, ctx, c.db, c.scratchKey, 8, 3*time.Second)
	txns := writingTxns(allTxns)
	t.Logf("workload complete: %d txns total, %d with writes",
		len(allTxns), len(txns))

	waitForTxns(t, txns, getEvents, feedErrCh)
	cancelFeed()

	events := getEvents()
	validateEvents(t, txns, events, feedSpan)

	validateEnrichment(t, txns, events, feedSpan)
}
