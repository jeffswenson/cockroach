// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/errors"
)

// ErrApplyCycle is returned when topological sorting detects a cycle in the
// dependency graph between rows.
var ErrApplyCycle = errors.New("cycle detected in apply order")

type sortStatus int

const (
	statusVisiting sortStatus = 1
	statusVisited  sortStatus = 2
)

// sort topologically sorts the write set by the order writes should be
// applied. It uses ls.scratch for lock and status data.
func (ls *LockSynthesizer) sort(
	ctx context.Context, rows []ldrdecoder.DecodedRow,
) ([]ldrdecoder.DecodedRow, error) {
	s := &ls.scratch

	// Grow and zero sortStatus.
	if cap(s.sortStatus) < len(rows) {
		s.sortStatus = make([]sortStatus, len(rows))
	} else {
		s.sortStatus = s.sortStatus[:len(rows)]
		clear(s.sortStatus)
	}

	sorted := make([]ldrdecoder.DecodedRow, 0, len(rows))
	for i := range rows {
		err := ls.sortInner(ctx, i, rows, &sorted)
		if err != nil {
			return nil, err
		}
	}
	return sorted, nil
}

func (ls *LockSynthesizer) sortInner(
	ctx context.Context, row int, rows []ldrdecoder.DecodedRow, output *[]ldrdecoder.DecodedRow,
) error {
	s := &ls.scratch

	if s.sortStatus[row] == statusVisited {
		return nil
	}
	if s.sortStatus[row] == statusVisiting {
		return ErrApplyCycle
	}

	s.sortStatus[row] = statusVisiting

	// This is mostly a standard topological sort with one quirk. We are using
	// the lock set to identify possible dependencies. So we need to
	// additionally filter with dependsOn to ensure there is a real edge between
	// the rows. If we allow for hash conflicts here, we would reject the
	// transaction as containing a cycle when we can actually apply it.
	//
	// For most of LDR, its okay if we have a lock hash conflict because it
	// results in a spurious dependency between transactions. It can't result in
	// a cycle because one of the transactions must come first in mvcc time.
	for j := s.lockOffsets[row]; j < s.lockOffsets[row+1]; j++ {
		lock := s.lockBuf[j]
		lr := &s.lockEntries[s.lockMap[lock.Hash]]
		for _, dependentRow := range lr.rows {
			if dependentRow == int32(row) {
				continue
			}
			dep, err := ls.dependsOn(ctx, rows[row], rows[dependentRow])
			if err != nil {
				return err
			}
			if !dep {
				continue
			}
			err = ls.sortInner(ctx, int(dependentRow), rows, output)
			if err != nil {
				return err
			}
		}
	}

	s.sortStatus[row] = statusVisited
	*output = append(*output, rows[row])
	return nil
}
