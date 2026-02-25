// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Lock represents a lock that must be acquired before applying a row mutation.
type Lock struct {
	Hash uint64
	// TODO(jeffswenson): Read will be set to true for foreign key locks that
	// only require a shared/read lock rather than an exclusive/write lock.
	Read bool
}

// LockSet contains the set of locks and the topologically sorted rows for a
// transaction.
type LockSet struct {
	Locks      []Lock
	SortedRows []ldrdecoder.DecodedRow
}

// deriveScratch holds reusable buffers that are reset between DeriveLocks
// calls. This avoids per-call allocations for all intermediate state, so that
// the only steady-state allocations are the two output slices (Locks and
// SortedRows).
type deriveScratch struct {
	// lockBuf is a flat buffer holding all locks for all rows. Row i's locks
	// are lockBuf[lockOffsets[i]:lockOffsets[i+1]].
	lockBuf     []Lock
	lockOffsets []int

	// lockMap maps lock hashes to indices in lockEntries.
	lockMap     map[uint64]int
	lockEntries []lockWithRows

	// sortStatus tracks visit state during topological sort.
	sortStatus []sortStatus
}

// LockSynthesizer derives locks and topological ordering for transaction rows.
// It is not safe for concurrent use; each goroutine must have its own instance.
type LockSynthesizer struct {
	tableConstraints map[descpb.ID]*tableConstraints
	scratch          deriveScratch
}

// NewLockSynthesizer creates a LockSynthesizer by acquiring leases for all
// destination tables and building their constraint metadata.
func NewLockSynthesizer(
	ctx context.Context,
	evalCtx *eval.Context,
	lm *lease.Manager,
	clock *hlc.Clock,
	tableMappings []ldrdecoder.TableMapping,
) (*LockSynthesizer, error) {
	ls := &LockSynthesizer{
		tableConstraints: make(
			map[descpb.ID]*tableConstraints, len(tableMappings),
		),
	}

	timestamp := lease.TimestampToReadTimestamp(clock.Now())
	for _, mapping := range tableMappings {
		leasedDesc, err := lm.Acquire(ctx, timestamp, mapping.DestID)
		if err != nil {
			return nil, err
		}

		tableDesc, ok := leasedDesc.Underlying().(catalog.TableDescriptor)
		if !ok {
			leasedDesc.Release(ctx)
			return nil, errors.Newf(
				"expected table descriptor for %d, got %T",
				mapping.DestID, leasedDesc.Underlying(),
			)
		}
		tc, err := newTableConstraints(evalCtx, tableDesc)
		if err != nil {
			leasedDesc.Release(ctx)
			return nil, err
		}
		ls.tableConstraints[mapping.DestID] = tc

		leasedDesc.Release(ctx)
	}

	return ls, nil
}

// lockWithRows tracks which rows share a particular lock hash.
type lockWithRows struct {
	rows    []int32
	scratch [8]int32
	isRead  bool
}

// DeriveLocks computes the locks required for a set of transaction rows and
// returns them in topologically sorted order. The only steady-state
// allocations are the two output slices (Locks and SortedRows).
func (ls *LockSynthesizer) DeriveLocks(
	ctx context.Context, rows []ldrdecoder.DecodedRow,
) (LockSet, error) {
	s := &ls.scratch

	// Reset scratch buffers.
	s.lockBuf = s.lockBuf[:0]
	s.lockEntries = s.lockEntries[:0]
	if s.lockMap == nil {
		s.lockMap = make(map[uint64]int)
	} else {
		clear(s.lockMap)
	}

	// Grow lockOffsets to len(rows)+1.
	if cap(s.lockOffsets) < len(rows)+1 {
		s.lockOffsets = make([]int, len(rows)+1)
	} else {
		s.lockOffsets = s.lockOffsets[:len(rows)+1]
	}
	s.lockOffsets[0] = 0

	for i, row := range rows {
		var err error
		s.lockBuf, err = ls.appendLocks(ctx, row, s.lockBuf)
		if err != nil {
			return LockSet{}, err
		}
		s.lockOffsets[i+1] = len(s.lockBuf)

		for j := s.lockOffsets[i]; j < s.lockOffsets[i+1]; j++ {
			lock := s.lockBuf[j]
			idx, ok := s.lockMap[lock.Hash]
			if !ok {
				idx = len(s.lockEntries)
				s.lockEntries = append(s.lockEntries, lockWithRows{
					isRead: lock.Read,
				})
				entry := &s.lockEntries[idx]
				entry.rows = entry.scratch[:0]
				entry.rows = append(entry.rows, int32(i))
				s.lockMap[lock.Hash] = idx
				continue
			}
			lr := &s.lockEntries[idx]
			if !lock.Read && lr.isRead {
				lr.isRead = false
			}
			if lr.rows[len(lr.rows)-1] != int32(i) {
				lr.rows = append(lr.rows, int32(i))
			}
		}
	}

	sorted, err := ls.sort(ctx, rows)
	if err != nil {
		return LockSet{}, err
	}

	lockSet := LockSet{
		SortedRows: sorted,
		Locks:      make([]Lock, 0, len(s.lockMap)),
	}
	for hash, idx := range s.lockMap {
		lockSet.Locks = append(lockSet.Locks, Lock{
			Hash: hash,
			Read: s.lockEntries[idx].isRead,
		})
	}

	return lockSet, nil
}

// appendLocks appends the locks for a single row to the provided slice.
func (ls *LockSynthesizer) appendLocks(
	ctx context.Context, row ldrdecoder.DecodedRow, locks []Lock,
) ([]Lock, error) {
	tc, ok := ls.tableConstraints[row.TableID]
	if !ok {
		return locks, nil
	}
	return tc.deriveLocks(ctx, row, locks)
}

// dependsOn returns true if row a must be applied before row b.
func (ls *LockSynthesizer) dependsOn(
	ctx context.Context, a, b ldrdecoder.DecodedRow,
) (bool, error) {
	if a.TableID != b.TableID {
		return false, nil
	}
	tc, ok := ls.tableConstraints[a.TableID]
	if !ok {
		return false, nil
	}
	return tc.DependsOn(ctx, a, b)
}
