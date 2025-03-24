// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkmerge

import (
	"bytes"
	"math/rand"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/errors"
)

type indexEntry struct {
	// idx is the index of the SST in the `files` list.
	idx int
	// isEnd is true if the index entry is the end of an SST. Each SST is in the index twice,
	// once as the start and once as the end.
	isEnd bool
}

type sstIndex struct {
	// files is a list of SSTs sorted by start key.
	files []execinfrapb.BulkMergeSpec_SST

	index []indexEntry
}

func (s *sstIndex) keyAt(idx int) roachpb.Key {
	if s.index[idx].isEnd {
		return s.files[s.index[idx].idx].EndKey
	}
	return s.files[s.index[idx].idx].StartKey
}

// groupBySpans returns a list of spans that overlap with each input span. The
// input spans must be sorted by start key and are non-overlapping.
//
// For example:
// output := groupBySpans(spans)
// output[i] contains all of the SSTs that overlap with spans[i].
func (s *sstIndex) groupBySpan(spans []roachpb.Span) [][]execinfrapb.BulkMergeSpec_SST {
	result := make([][]execinfrapb.BulkMergeSpec_SST, len(spans))
	index := s.index

	// overlapping is the set of SSts that overlap with the current span. The
	// entry is false if the SST span is open (i.e. we have not yet seen the
	// end), or true if the SST span is closed (i.e. we have seen the end).
	overlapping := make(map[int]bool)
	for i, span := range spans {
		// remove closed SSTs from the map
		for idx, isEnd := range overlapping {
			if isEnd {
				delete(overlapping, idx)
			}
		}

		// Loop over index until we find an SST that starts or ends at the span

		// add open SSTs to the map

		overlapping := make([]execinfrapb.BulkMergeSpec_SST, 0)

		// Iterate through all SSTs
		for _, sst := range s.files {
			// Check if the SST overlaps with the span
			// SST overlaps if:
			// 1. SST start key < span end key AND
			// 2. SST end key > span start key
			if bytes.Compare(sst.StartKey, span.EndKey) < 0 && bytes.Compare(sst.EndKey, span.Key) > 0 {
				overlapping = append(overlapping, sst)
			}
		}

		result[i] = overlapping
	}

	return result
}

func makeSSTIndex(files []bulksst.SSTFiles) sstIndex {
	// Calculate total number of SSTs
	totalSSTs := 0
	for _, file := range files {
		totalSSTs += len(file.SST)
	}

	// Pre-allocate result slices
	result := sstIndex{
		files: make([]execinfrapb.BulkMergeSpec_SST, 0, totalSSTs),
		index: make([]indexEntry, 0, totalSSTs*2), // *2 because each SST has start and end entries
	}

	// First collect all SSTs into the files slice
	for _, file := range files {
		for _, sst := range file.SST {
			result.files = append(result.files, execinfrapb.BulkMergeSpec_SST{
				StartKey: []byte(sst.StartKey),
				EndKey:   []byte(sst.EndKey),
				Uri:      sst.URI,
			})
		}
	}

	// Sort SSTs by start key
	slices.SortFunc(result.files, func(a, b execinfrapb.BulkMergeSpec_SST) int {
		return bytes.Compare(a.StartKey, b.StartKey)
	})

	// Build the index with both start and end entries
	for i := range result.files {
		// Add start entry
		result.index = append(result.index, indexEntry{
			idx:   i,
			isEnd: false,
		})
		// Add end entry
		result.index = append(result.index, indexEntry{
			idx:   i,
			isEnd: true,
		})
	}

	// Sort index entries by key (start or end) position
	slices.SortFunc(result.index, func(a, b indexEntry) int {
		// TODO(jeffswenson): These keys should never be equal, but we need to
		// ensure the merge process identifies that case and returns an error to
		// the user.  It would indicate a primary key violation for the imported
		// date.  It doesn't make sense to validate that here because the
		// duplicate keys could also be inside the SST.
		keyA := result.files[a.idx].EndKey
		keyB := result.files[b.idx].EndKey
		if !a.isEnd {
			keyA = result.files[a.idx].StartKey
		}
		if !b.isEnd {
			keyB = result.files[b.idx].StartKey
		}
		return bytes.Compare(keyA, keyB)
	})

	return result
}

type MergeGroup struct {
	Files []execinfrapb.BulkMergeSpec_SST
	Spans []roachpb.Span
	// Ingest is true if the merge group should be written to KV and false if it
	// should be flushed to intermediate storage
	Ingest bool
}

func SplitMergeGroups(
	files []bulksst.SSTFiles, tableSpans []roachpb.Span,
) []MergeGroup {
	group, ok := tryOnePhaseMerge(files, tableSpans)
	if ok {
		return group
	}

	panic("multi-phase merge not implemented")
}

// tryOnePhaseMerge attempts to build groups of 1K SSTs that can be directly
// ingested into the KV layer. This requires that the input SST files are mostly
// disjoint.
func tryOnePhaseMerge(
	files []bulksst.SSTFiles, tableSpan roachpb.Span,
) ([]MergeGroup, bool) {
	// What if I have a list of SST file start and end keys?
	// While iteratoring over the SST files I can maintain a map of SST files
	// that overlap with the current Each SST file in the map has a "closed"
	// flag that is set to true when the When we create a merge group for a
	// span, we can drop all of the closed ssts from the map, but we need to
	// keep the open ssts since they overlap with the next span.
	panic("not implemented")
}

// CombineFileInfo combines the SST files and picks splits based on the key sample.
func CombineFileInfo(
	files []bulksst.SSTFiles, tableSpans []roachpb.Span,
) ([]execinfrapb.BulkMergeSpec_SST, []roachpb.Span) {
	totalSize := uint64(0)
	result := make([]execinfrapb.BulkMergeSpec_SST, 0)
	samples := make([]roachpb.Key, 0)
	for _, file := range files {
		for _, sst := range file.SST {
			totalSize += sst.FileSize
			result = append(result, execinfrapb.BulkMergeSpec_SST{
				StartKey: []byte(sst.StartKey),
				EndKey:   []byte(sst.EndKey),
				Uri:      sst.URI,
			})
		}
		for _, sample := range file.RowSamples {
			samples = append(samples, roachpb.Key(sample))
		}
	}

	shuffle(samples)

	targetSize := uint64(256 << 20)
	samples = samples[:totalSize/targetSize]

	// BUGFIX: We need to sort the samples for merge, otherwise the merge can end
	// up with overlapping spans and duplicate data.
	slices.SortFunc(samples, func(i, j roachpb.Key) int {
		return bytes.Compare(i, j)
	})

	slices.SortFunc(tableSpans, func(i, j roachpb.Span) int {
		return bytes.Compare(i.Key, j.Key)
	})

	spans := getMergeSpans(tableSpans, samples)

	return result, spans
}

func shuffle[T any](s []T) {
	for i := range s {
		j := rand.Intn(len(s))
		s[i], s[j] = s[j], s[i]
	}
}

// getMergeSpans determines which spans should be used as merge tasks. The
// output spans must fully cover the input spans. The samples are used to
// determine where schema spans should be split.
func getMergeSpans(schemaSpans []roachpb.Span, sortedSample []roachpb.Key) []roachpb.Span {
	// TODO(jeffswenson): validate that every sample is contained within a schema span
	result := make([]roachpb.Span, 0, len(schemaSpans)+len(sortedSample))

	for _, span := range schemaSpans {
		samples := getCoveredSamples(span, sortedSample)
		sortedSample = sortedSample[len(samples):]

		startKey := span.Key
		for _, sample := range samples {
			result = append(result, roachpb.Span{
				Key:    startKey,
				EndKey: sample,
			})
			startKey = sample
		}
		result = append(result, roachpb.Span{
			Key:    startKey,
			EndKey: span.EndKey,
		})
	}

	return result
}

func getCoveredSamples(span roachpb.Span, sortedSamples []roachpb.Key) []roachpb.Key {
	for i, sample := range sortedSamples {
		if bytes.Compare(span.EndKey, sample) <= 0 {
			return sortedSamples[:i]
		}
	}
	return sortedSamples
}
