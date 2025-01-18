package sstmerge

import (
	"bytes"
	"slices"
)

// KeySample is produced for each input SST for a merge phase. SampledKeys
// should be picked randomly from the keys in the table.
type KeySample struct {
	SampledKeys [][]byte
	TableSize   uint64
}

type Split struct {
	Start         []byte
	EstimatedSize uint64
}

// PickSplits takes a set of key samples and a target size for output SSTs and
// returns a set of splits that will produce SSTs with an average size equal to
// the target size.
//
// targetSplitSize is expected to be ~10x the target sst size. Splits need to
// be small enough that load is distributed evenly and large enough that most
// SSTs are large.
func PickSplits(tables []KeySample, targetSplitSize uint64) []Split {
	// We can estimate the quality of this strategy using simulation.

	// TODO: test sequential keys in sorted SSTs
	// TODO: test random keys
	// TODO: test a consistent prefix
	// TODO: test a sequential id with random cohort sizes (e.g. conversations with random length)

	// For each sampled key, estimate its size as (table size / sampled keys).
	// Sort the keys
	// Sum estimated bytes until we reach the target size
	// Then pick the last key that has an estimated size less than the target size
	var candidateSplits []Split
	for _, table := range tables {
		estimatedSize := table.TableSize / (uint64(len(table.SampledKeys)) + 1)
		for _, key := range table.SampledKeys {
			candidateSplits = append(candidateSplits, Split{key, estimatedSize})
		}
	}

	slices.SortFunc(candidateSplits, func(i, j Split) int {
		return bytes.Compare(i.Start, j.Start)
	})

	var splits []Split

	var start []byte
	estimatedSize := uint64(0)
	for _, split := range candidateSplits {
		estimatedSize += split.EstimatedSize
		if targetSplitSize <= estimatedSize {
			splits = append(splits, Split{Start: start, EstimatedSize: estimatedSize})
		}
	}

	return splits
}
