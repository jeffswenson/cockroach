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

// TestPushTxnTxnFeedOps tests that PushTxn emits the correct TxnFeedOps
// depending on the push type and whether the txnfeed setting is enabled.
func TestPushTxnTxnFeedOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	now := clock.Now()

	tests := []struct {
		name           string
		pushType       kvpb.PushTxnType
		txnFeedEnabled bool
		expectedOpType kvserverpb.TxnFeedOp_Type
		expectOps      bool
	}{
		{
			name:           "push abort, txnfeed enabled",
			pushType:       kvpb.PUSH_ABORT,
			txnFeedEnabled: true,
			expectedOpType: kvserverpb.TxnFeedOp_ABORTED,
			expectOps:      true,
		},
		{
			name:           "push abort, txnfeed disabled",
			pushType:       kvpb.PUSH_ABORT,
			txnFeedEnabled: false,
			expectOps:      false,
		},
		{
			name:           "push timestamp, txnfeed enabled",
			pushType:       kvpb.PUSH_TIMESTAMP,
			txnFeedEnabled: true,
			expectOps:      false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engine := storage.NewDefaultInMemForTesting()
			defer engine.Close()

			st := cluster.MakeTestingClusterSettings()
			kvserverbase.TxnFeedEnabled.Override(ctx, &st.SV, tc.txnFeedEnabled)

			key := roachpb.Key("foo")
			pusheeTxnID := uuid.MakeV4()
			pusheeTxnMeta := enginepb.TxnMeta{
				ID:             pusheeTxnID,
				Key:            key,
				MinTimestamp:   now,
				WriteTimestamp: now,
			}

			// Write a PENDING transaction record so PushTxn can find and push it.
			txnKey := keys.TransactionKey(key, pusheeTxnID)
			txnRecord := roachpb.TransactionRecord{
				TxnMeta: pusheeTxnMeta,
				Status:  roachpb.PENDING,
			}
			require.NoError(t, storage.MVCCPutProto(
				ctx, engine, txnKey, now, &txnRecord,
				storage.MVCCWriteOptions{},
			))

			evalCtx := (&batcheval.MockEvalCtx{
				Clock:           clock,
				ClusterSettings: st,
				CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
					return true, 0
				},
			}).EvalContext()

			resp := kvpb.PushTxnResponse{}
			res, err := batcheval.PushTxn(ctx, engine, batcheval.CommandArgs{
				EvalCtx: evalCtx,
				Header: kvpb.Header{
					Timestamp: clock.Now(),
				},
				Args: &kvpb.PushTxnRequest{
					RequestHeader: kvpb.RequestHeader{Key: key},
					PusheeTxn:     pusheeTxnMeta,
					PushType:      tc.pushType,
					PushTo:        now.Next(),
					Force:         true, // force the push to succeed
				},
			}, &resp)
			require.NoError(t, err)

			if tc.expectOps {
				require.NotNil(t, res.Replicated.TxnFeedOps)
				require.Len(t, res.Replicated.TxnFeedOps.Ops, 1)
				op := res.Replicated.TxnFeedOps.Ops[0]
				require.Equal(t, tc.expectedOpType, op.Type)
				require.Equal(t, pusheeTxnID, op.TxnID)
				require.Equal(t, key, op.AnchorKey)
			} else {
				require.Nil(t, res.Replicated.TxnFeedOps)
			}
		})
	}
}
