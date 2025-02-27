// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst_test

import (
	"context"
	"fmt"
	math "math"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func encodeKey(strKey string) []byte {
	key := storage.MVCCKey{
		Key: []byte(strKey),
	}
	return storage.EncodeMVCCKeyToBuf(nil, key)
}

func TestBulkSSTWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const writePath = "test_gsst"
	fs := s.StorageLayer().Engines()[0].Env()

	fileAllocator := bulksst.NewVFSFileAllocator(writePath, fs, s.ClusterSettings())
	// Limit the size of individual SSTs to 1KB.
	bulksst.BatchSize.Override(ctx, &s.ClusterSettings().SV, 1024)
	// Limit the size of the row samples to be only 1KB.
	bulksst.MaxRowSampleBytes.Override(ctx, &s.ClusterSettings().SV, 1024)
	// Create a new batcher
	batcher := bulksst.NewUnsortedSSTBatcher(s.ClusterSettings(), fileAllocator)

	// Intentionally go in an unsorted order.
	expectedSet := intsets.MakeFast()
	var maxKey roachpb.Key
	var minKey roachpb.Key
	for i := 8192; i > 0; i-- {
		key := roachpb.Key(encodeKey(fmt.Sprintf("key-%d", i)))
		// Track the min and max across all SST files.
		if len(minKey) == 0 || key.Compare(minKey) < 0 {
			minKey = key
		}
		if len(maxKey) == 0 || key.Compare(maxKey) > 0 {
			maxKey = key
		}
		require.NoError(t, batcher.AddMVCCKey(ctx, storage.MVCCKey{
			Timestamp: s.Clock().Now(),
			Key:       key,
		},
			[]byte(fmt.Sprintf("value-%d", i))))
		expectedSet.Add(i)
	}
	totalSpan := roachpb.Span{Key: minKey, EndKey: maxKey.Next()}
	require.NoError(t, batcher.CloseWithError(ctx))

	// Next validate each SST file.
	set := intsets.MakeFast()
	lastFileMin := -1
	sstFiles := fileAllocator.GetFileList()
	// Sort the row samples and get spans for all the splits. We will use
	// these spans to determine which SST file a given key belongs to.
	sort.Slice(sstFiles.RowSamples, func(i, j int) bool {
		return roachpb.Key(sstFiles.RowSamples[i]).Compare(roachpb.Key(sstFiles.RowSamples[j])) < 0
	})
	rowSampleSpans := make([]roachpb.Span, len(sstFiles.RowSamples)+1)
	lastKey := roachpb.KeyMin
	spanIndex := 0
	for i, rowSample := range sstFiles.RowSamples {
		require.True(t, totalSpan.ContainsKey(roachpb.Key(rowSample)))
		rowSampleSpans[spanIndex] = roachpb.Span{Key: lastKey, EndKey: roachpb.Key(rowSample).Next()}
		require.True(t, rowSampleSpans[spanIndex].Valid())
		if i == len(sstFiles.RowSamples)-1 {
			rowSampleSpans[spanIndex+1] = roachpb.Span{Key: roachpb.Key(rowSample), EndKey: roachpb.KeyMax}
			require.True(t, rowSampleSpans[spanIndex+1].Valid())
		}
		lastKey = roachpb.Key(rowSample)
		spanIndex += 1
	}
	splitFromKey := func(key roachpb.Key) int {
		for i, rowSampleSpan := range rowSampleSpans {
			if rowSampleSpan.ContainsKey(key) {
				return i
			}
		}
		require.Failf(t, "key not found in any row sample span", "key: %v, row sample spans: %v\n", key, rowSampleSpans)
		return -1
	}
	sstSizePerSplit := make([]int, len(sstFiles.RowSamples)+1)
	for _, sstInfo := range sstFiles.SST {
		currFileMin := -1
		currFileMax := -1
		values := readKeyValuesFromSST(t, fs, sstInfo.URI)
		// Validate the span of the SST file.
		require.Equal(t, sstInfo.StartKey, string(values[0].Key.Key))
		require.Equal(t, sstInfo.EndKey, string(values[len(values)-1].Key.Key))
		for _, value := range values {
			keyString := string(value.Key.Key)
			var num int
			scanned, err := fmt.Sscanf(strings.Split(keyString, "-")[1], "%d", &num)
			require.NoError(t, err)
			require.Equal(t, 1, scanned)
			set.Add(num)
			if currFileMin == -1 || currFileMin > num {
				currFileMin = num
			}
			if currFileMax == -1 || currFileMax < num {
				currFileMax = num
			}
			sstSizePerSplit[splitFromKey(value.Key.Key)] += len(value.Value) + len(value.Key.Key)
		}
		// Ensure continuity between SSTs, where the minimum on the
		// previous file should continue to this file.
		if lastFileMin > 0 {
			require.Equal(t, lastFileMin-1, currFileMax)
		}
		lastFileMin = currFileMin
	}
	// Validate the distribution of the SST splits.
	validateSSTDistribution(t, sstSizePerSplit)
	// Ensure we have all the inserted key / values.
	require.Equal(t, 8192, set.Len())
	require.Zero(t, expectedSet.Difference(set).Len())
}

// validateSSTDistribution validates that the split points end up being
// normally distributed.
func validateSSTDistribution(t *testing.T, sstSizePerSplit []int) {
	// Validate that the distribution of SST sizes in this scenario.
	sort.Ints(sstSizePerSplit)
	averageSstSizePerSplit := 0
	for _, size := range sstSizePerSplit {
		averageSstSizePerSplit += size
	}
	averageSstSizePerSplit /= len(sstSizePerSplit)
	// Calculate standard deviation
	variance := 0.0
	for _, size := range sstSizePerSplit {
		diff := float64(size - averageSstSizePerSplit)
		variance += diff * diff
	}
	variance /= float64(len(sstSizePerSplit))
	stdDev := math.Sqrt(variance)
	// Check if data follows the empirical rule for normal distribution
	// Count data points within 1, 2, and 3 standard deviations
	oneStdCount, twoStdCount, threeStdCount := 0, 0, 0
	for _, size := range sstSizePerSplit {
		diff := math.Abs(float64(size - averageSstSizePerSplit))
		if diff <= stdDev {
			oneStdCount++
		}
		if diff <= 2*stdDev {
			twoStdCount++
		}
		if diff <= 3*stdDev {
			threeStdCount++
		}
	}
	// For normal distribution:
	// ~68% of data should be within 1 standard deviation
	// ~95% within 2 standard deviations
	// ~99.7% within 3 standard deviations
	totalCount := float64(len(sstSizePerSplit))
	oneStdPct := float64(oneStdCount) / totalCount
	twoStdPct := float64(twoStdCount) / totalCount
	threeStdPct := float64(threeStdCount) / totalCount
	// Allow for some deviation from perfect normal distribution
	require.InDelta(t, 0.68, oneStdPct, 0.5, "~68%% of data should be within 1 standard deviation")
	require.InDelta(t, 0.95, twoStdPct, 0.25, "~95%% of data should be within 2 standard deviations")
	require.InDelta(t, 0.997, threeStdPct, 0.125, "~99.7%% of data should be within 3 standard deviations")
}
func readKeyValuesFromSST(t *testing.T, fs vfs.FS, filename string) []storage.MVCCKeyValue {
	file, err := fs.Open(filename, nil)
	require.NoError(t, err)
	readable, err := sstable.NewSimpleReadable(file)
	require.NoError(t, err)

	reader, err := sstable.NewReader(
		context.Background(),
		readable,
		storage.DefaultPebbleOptions().MakeReaderOptions())
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	iter, err := reader.NewIter(sstable.NoTransforms, nil, nil)
	if err != nil {

	}
	defer func() {
		require.NoError(t, iter.Close())
	}()

	result := make([]storage.MVCCKeyValue, 0)
	for internalKV := iter.First(); internalKV != nil; internalKV = iter.Next() {
		mvccKey, err := storage.DecodeMVCCKey(internalKV.K.UserKey)
		require.NoError(t, err)
		rawValue, _, err := internalKV.V.Value(nil)
		require.NoError(t, err)
		result = append(result, storage.MVCCKeyValue{
			Key:   mvccKey.Clone(),
			Value: rawValue,
		})
	}
	return result
}
