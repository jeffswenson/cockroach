// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ApplierID identifies which applier owns a transaction. Used to partition
// transactions across parallel appliers and coordinate cross-applier
// dependencies.
type ApplierID int32

// TxnID uniquely identifies a transaction. The UUID is the primary identity,
// while the Timestamp provides ordering. Comparison methods (Less, LessEq)
// delegate to the underlying Timestamp, so TxnIDs are ordered by timestamp.
type TxnID struct {
	Timestamp hlc.Timestamp
	UUID      uuid.UUID
}

func (t TxnID) Less(s TxnID) bool { return t.Timestamp.Less(s.Timestamp) }

func (t TxnID) LessEq(s TxnID) bool { return t.Timestamp.LessEq(s.Timestamp) }

func (t TxnID) IsSet() bool { return t.Timestamp.IsSet() }

func (t TxnID) String() string {
	return fmt.Sprintf("(%s,%d)", t.UUID.Short(), t.Timestamp.WallTime)
}

// ApplierRouter determines which applier owns a transaction based on its UUID.
type ApplierRouter interface {
	Route(id uuid.UUID) ApplierID
}

// HashRouter routes transactions to appliers by hashing the UUID.
type HashRouter struct {
	applierIDs []ApplierID
}

// NewHashRouter creates a router that distributes transactions across appliers
// using a hash of the UUID.
func NewHashRouter(ids []ApplierID) *HashRouter {
	return &HashRouter{applierIDs: ids}
}

// Route picks an applier for the given UUID by hashing.
func (r *HashRouter) Route(id uuid.UUID) ApplierID {
	h := binary.BigEndian.Uint64(id[:8])
	return r.applierIDs[h%uint64(len(r.applierIDs))]
}

// MakeTxnIDFromTimestamp creates a deterministic TxnID from an MVCC timestamp.
// Used by the scheduler-based path where transactions don't have source UUIDs.
func MakeTxnIDFromTimestamp(ts hlc.Timestamp) TxnID {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(ts.WallTime))
	binary.BigEndian.PutUint32(buf[8:12], uint32(ts.Logical))
	return TxnID{
		Timestamp: ts,
		UUID:      uuid.FromBytesOrNil(buf[:]),
	}
}

type Transaction struct {
	TxnID    TxnID
	WriteSet []DecodedRow
}

type TxnDecoder struct {
	decoder tableDecoder
}

func NewTxnDecoder(
	ctx context.Context, descriptors descs.DB, settings *cluster.Settings, tables []TableMapping,
) (*TxnDecoder, error) {
	decoder, err := newTableDecoder(ctx, descriptors, settings, tables)
	if err != nil {
		return nil, err
	}
	return &TxnDecoder{
		decoder: decoder,
	}, nil
}

func (t *TxnDecoder) DecodeTxn(
	ctx context.Context, transaction []streampb.StreamEvent_KV,
) (Transaction, error) {
	if len(transaction) == 0 {
		return Transaction{}, errors.AssertionFailedf("empty transaction")
	}

	var result Transaction
	result.TxnID = MakeTxnIDFromTimestamp(transaction[0].KeyValue.Value.Timestamp)
	result.WriteSet = make([]DecodedRow, 0, len(transaction))

	for _, event := range transaction {
		decoded, _, err := t.decoder.decodeEvent(ctx, event)
		if err != nil {
			return Transaction{}, err
		}
		if decoded.RowTimestamp != result.TxnID.Timestamp {
			return Transaction{}, errors.AssertionFailedf("inconsistent timestamps in transaction: got %s, expected %s",
				decoded.RowTimestamp, result.TxnID.Timestamp)
		}
		result.WriteSet = append(result.WriteSet, decoded)
	}

	return result, nil
}
