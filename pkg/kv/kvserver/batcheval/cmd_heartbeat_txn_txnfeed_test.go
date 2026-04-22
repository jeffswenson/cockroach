// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestHeartbeatTxnTxnFeedOps tests that HeartbeatTxn emits the correct
// TxnFeedOps depending on the transaction state and whether the txnfeed
// setting is enabled.
func TestHeartbeatTxnTxnFeedOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	now := clock.Now()

	key := roachpb.Key("a")

	tests := []struct {
		name           string
		existingTxn    *roachpb.Transaction // nil = no existing record
		txnFeedEnabled bool
		closedTS       hlc.Timestamp
		expectOps      bool
	}{
		{
			name:           "new record, txnfeed enabled",
			txnFeedEnabled: true,
			expectOps:      true,
		},
		{
			name:           "new record, txnfeed disabled",
			txnFeedEnabled: false,
			expectOps:      false,
		},
		{
			name: "existing pending record, txnfeed enabled",
			existingTxn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: now,
					MinTimestamp:   now,
				},
				Status: roachpb.PENDING,
			},
			txnFeedEnabled: true,
			expectOps:      true,
		},
		{
			name: "existing committed record, txnfeed enabled",
			existingTxn: &roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{
					WriteTimestamp: now,
					MinTimestamp:   now,
				},
				Status: roachpb.COMMITTED,
			},
			txnFeedEnabled: true,
			// Finalized records don't get heartbeated — the code checks
			// !txn.Status.IsFinalized(). No TxnFeedOps emitted.
			expectOps: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine := storage.NewDefaultInMemForTesting()
			defer engine.Close()

			st := cluster.MakeTestingClusterSettings()
			kvserverbase.TxnFeedEnabled.Override(ctx, &st.SV, tc.txnFeedEnabled)

			// Create the header transaction. HeartbeatTxn uses h.Txn.Key and
			// h.Txn.ID to look up the transaction record.
			headerTxn := roachpb.MakeTransaction(
				"test", key, 0, 0, now, 0, 1, 0, false, /* omitInRangefeeds */
			)
			txnID := headerTxn.ID

			// Write existing transaction record if provided. Use the same txnID
			// so HeartbeatTxn can find it.
			if tc.existingTxn != nil {
				existing := *tc.existingTxn
				existing.ID = txnID
				existing.Key = key
				txnKey := keys.TransactionKey(key, txnID)
				require.NoError(t, storage.MVCCPutProto(
					ctx, engine, txnKey, now.Prev(), &existing,
					storage.MVCCWriteOptions{},
				))
			}

			evalCtx := (&batcheval.MockEvalCtx{
				Clock:           clock,
				ClusterSettings: st,
				ClosedTimestamp: tc.closedTS,
			}).EvalContext()

			resp := kvpb.HeartbeatTxnResponse{}
			res, err := batcheval.HeartbeatTxn(ctx, engine, batcheval.CommandArgs{
				EvalCtx: evalCtx,
				Header: kvpb.Header{
					Timestamp: now,
					Txn:       &headerTxn,
				},
				Args: &kvpb.HeartbeatTxnRequest{
					RequestHeader: kvpb.RequestHeader{Key: key},
					Now:           now,
				},
			}, &resp)
			require.NoError(t, err)

			if tc.expectOps {
				require.NotNil(t, res.Replicated.TxnFeedOps)
				require.Len(t, res.Replicated.TxnFeedOps.Ops, 1)
				op := res.Replicated.TxnFeedOps.Ops[0]
				require.Equal(t, kvserverpb.TxnFeedOp_RECORD_WRITTEN, op.Type)
				require.Equal(t, txnID, op.TxnID)
				require.Equal(t, key, op.AnchorKey)
			} else {
				require.Nil(t, res.Replicated.TxnFeedOps)
			}
		})
	}
}

// TestHeartbeatTxnWriteTimestampPushedAboveClosedTS verifies that when creating
// a new transaction record with txnfeed enabled, the WriteTimestamp is bumped
// above the closed timestamp.
func TestHeartbeatTxnWriteTimestampPushedAboveClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	now := clock.Now()

	txnID := uuid.MakeV4()
	key := roachpb.Key("a")

	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	st := cluster.MakeTestingClusterSettings()
	kvserverbase.TxnFeedEnabled.Override(ctx, &st.SV, true)

	// Set closed timestamp above the transaction's write timestamp.
	closedTS := now.Add(10, 0)
	evalCtx := (&batcheval.MockEvalCtx{
		Clock:           clock,
		ClusterSettings: st,
		ClosedTimestamp: closedTS,
	}).EvalContext()

	headerTxn := roachpb.MakeTransaction(
		"test", key, 0, 0, now, 0, 1, 0, false, /* omitInRangefeeds */
	)
	headerTxn.ID = txnID

	resp := kvpb.HeartbeatTxnResponse{}
	res, err := batcheval.HeartbeatTxn(ctx, engine, batcheval.CommandArgs{
		EvalCtx: evalCtx,
		Header: kvpb.Header{
			Timestamp: now,
			Txn:       &headerTxn,
		},
		Args: &kvpb.HeartbeatTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: key},
			Now:           now,
		},
	}, &resp)
	require.NoError(t, err)

	// The RECORD_WRITTEN op should reflect the bumped timestamp.
	require.NotNil(t, res.Replicated.TxnFeedOps)
	op := res.Replicated.TxnFeedOps.Ops[0]
	require.Equal(t, kvserverpb.TxnFeedOp_RECORD_WRITTEN, op.Type)
	// WriteTimestamp should have been pushed above closedTS.
	require.True(t, closedTS.Less(op.WriteTimestamp),
		"expected WriteTimestamp %s to be above closedTS %s",
		op.WriteTimestamp, closedTS)
}
