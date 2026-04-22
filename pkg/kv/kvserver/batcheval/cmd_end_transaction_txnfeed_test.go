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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestEndTxnTxnFeedOps tests that EndTxn emits the correct TxnFeedOps for
// commit, abort, and staging transitions.
func TestEndTxnTxnFeedOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	as := abortspan.New(desc.RangeID)

	txnKey := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction(
		"test", txnKey, 0, 0, ts, 0, 1, 0, false, /* omitInRangefeeds */
	)
	lockSpans := []roachpb.Span{{Key: roachpb.Key("b")}}
	readSpans := []roachpb.Span{{Key: roachpb.Key("c")}}
	writes := []roachpb.SequencedWrite{{Key: txnKey, Sequence: 0}}

	tests := []struct {
		name           string
		existingTxn    *roachpb.TransactionRecord // nil = create new record
		commit         bool
		inFlightWrites []roachpb.SequencedWrite // non-nil = parallel commit (staging)
		txnFeedEnabled bool
		expectedOpType kvserverpb.TxnFeedOp_Type
		expectOps      bool
	}{
		{
			name:           "commit, txnfeed enabled",
			existingTxn:    nil,
			commit:         true,
			txnFeedEnabled: true,
			expectedOpType: kvserverpb.TxnFeedOp_COMMITTED,
			expectOps:      true,
		},
		{
			name:           "commit, txnfeed disabled",
			existingTxn:    nil,
			commit:         true,
			txnFeedEnabled: false,
			expectOps:      false,
		},
		{
			name:           "abort, txnfeed enabled",
			existingTxn:    nil,
			commit:         false,
			txnFeedEnabled: true,
			expectedOpType: kvserverpb.TxnFeedOp_ABORTED,
			expectOps:      true,
		},
		{
			name:           "abort, txnfeed disabled",
			existingTxn:    nil,
			commit:         false,
			txnFeedEnabled: false,
			expectOps:      false,
		},
		{
			name:           "stage (parallel commit), txnfeed enabled",
			existingTxn:    nil,
			commit:         true,
			inFlightWrites: writes,
			txnFeedEnabled: true,
			expectedOpType: kvserverpb.TxnFeedOp_RECORD_WRITTEN,
			expectOps:      true,
		},
		{
			name:           "stage (parallel commit), txnfeed disabled",
			existingTxn:    nil,
			commit:         true,
			inFlightWrites: writes,
			txnFeedEnabled: false,
			expectOps:      false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := storage.NewDefaultInMemForTesting()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()

			st := cluster.MakeTestingClusterSettings()
			kvserverbase.TxnFeedEnabled.Override(ctx, &st.SV, tc.txnFeedEnabled)

			// Write existing txn record if provided.
			txnRecordKey := keys.TransactionKey(txn.Key, txn.ID)
			if tc.existingTxn != nil {
				require.NoError(t, storage.MVCCPutProto(
					ctx, batch, txnRecordKey, ts.Prev(), tc.existingTxn,
					storage.MVCCWriteOptions{},
				))
			}

			req := kvpb.EndTxnRequest{
				RequestHeader:  kvpb.RequestHeader{Key: txn.Key},
				Commit:         tc.commit,
				LockSpans:      lockSpans,
				ReadSpans:      readSpans,
				InFlightWrites: tc.inFlightWrites,
			}

			var resp kvpb.EndTxnResponse
			res, err := batcheval.EndTxn(ctx, batch, batcheval.CommandArgs{
				EvalCtx: (&batcheval.MockEvalCtx{
					Desc:            &desc,
					Clock:           clock,
					AbortSpan:       as,
					ClusterSettings: st,
					CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
						return true, 0
					},
				}).EvalContext(),
				Args: &req,
				Header: kvpb.Header{
					Timestamp: ts,
					Txn:       txn.Clone(),
				},
			}, &resp)
			require.NoError(t, err)

			if tc.expectOps {
				require.NotNil(t, res.Replicated.TxnFeedOps,
					"expected TxnFeedOps to be set")
				require.Len(t, res.Replicated.TxnFeedOps.Ops, 1)
				op := res.Replicated.TxnFeedOps.Ops[0]
				require.Equal(t, tc.expectedOpType, op.Type)
				require.Equal(t, txn.ID, op.TxnID)
				require.Equal(t, txnKey, op.AnchorKey)

				if op.Type == kvserverpb.TxnFeedOp_COMMITTED {
					require.Equal(t, lockSpans, op.WriteSpans)
					require.Equal(t, readSpans, op.ReadSpans)
				}
			} else {
				require.Nil(t, res.Replicated.TxnFeedOps,
					"expected no TxnFeedOps")
			}
		})
	}
}

// TestEndTxnWriteTimestampPushedAboveClosedTS verifies that when creating a new
// transaction record during commit with txnfeed enabled, the WriteTimestamp is
// bumped above the closed timestamp. For a serializable transaction, this push
// triggers a RETRY_SERIALIZABLE error because the WriteTimestamp diverges from
// the ReadTimestamp.
func TestEndTxnWriteTimestampPushedAboveClosedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))

	startKey := roachpb.Key("0000")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	as := abortspan.New(desc.RangeID)

	txnKey := roachpb.Key("a")
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction(
		"test", txnKey, 0, 0, ts, 0, 1, 0, false, /* omitInRangefeeds */
	)

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()
	batch := db.NewBatch()
	defer batch.Close()

	st := cluster.MakeTestingClusterSettings()
	kvserverbase.TxnFeedEnabled.Override(ctx, &st.SV, true)

	// Set closed timestamp above the transaction's write timestamp.
	closedTS := hlc.Timestamp{WallTime: 10}

	req := kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: txn.Key},
		Commit:        true,
		LockSpans:     []roachpb.Span{{Key: roachpb.Key("b")}},
	}

	var resp kvpb.EndTxnResponse
	_, err := batcheval.EndTxn(ctx, batch, batcheval.CommandArgs{
		EvalCtx: (&batcheval.MockEvalCtx{
			Desc:            &desc,
			Clock:           clock,
			AbortSpan:       as,
			ClusterSettings: st,
			ClosedTimestamp: closedTS,
			CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
				return true, 0
			},
		}).EvalContext(),
		Args: &req,
		Header: kvpb.Header{
			Timestamp: ts,
			Txn:       txn.Clone(),
		},
	}, &resp)

	// The write timestamp push above the closed timestamp causes the
	// serializable transaction to fail with RETRY_SERIALIZABLE.
	var retryErr *kvpb.TransactionRetryError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, kvpb.RETRY_SERIALIZABLE, retryErr.Reason)
}
