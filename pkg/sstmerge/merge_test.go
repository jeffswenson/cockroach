package sstmerge

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

type keyValue struct {
	key   string
	value string
}

func kv(key string, value string) keyValue {
	return keyValue{key: key, value: value}
}

func readSST(t *testing.T, os ObjectStore, filename string) []keyValue {
	file, err := os.Read(context.Background(), filename)
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

	var result []keyValue
	for internalKV := iter.First(); internalKV != nil; internalKV = iter.Next() {
		mvccKey, err := storage.DecodeMVCCKey(internalKV.K.UserKey)

		rawValue, _, err := internalKV.V.Value(nil)
		require.NoError(t, err)
		result = append(result, kv(string(mvccKey.Key), string(rawValue)))
	}

	return result
}

func encodeKey(strKey string) []byte {
	key := storage.MVCCKey{
		Key: []byte(strKey),
	}
	return storage.EncodeMVCCKeyToBuf(nil, key)
}

func writeSST(t *testing.T, os ObjectStore, filename string, data []keyValue) {
	writable, err := os.Write(context.Background(), filename)
	require.NoError(t, err)

	options := storage.DefaultPebbleOptions().MakeWriterOptions(0, sstable.TableFormatPebblev5)

	writer := sstable.NewWriter(writable, options)
	for _, kv := range data {
		key := storage.MVCCKey{
			Key: []byte(kv.key),
		}
		encoded := storage.EncodeMVCCKeyToBuf(nil, key)
		require.NoError(t, writer.Set(encoded, []byte(kv.value)))
	}
	require.NoError(t, writer.Close())
}

func TestReadWriteSST(t *testing.T) {
	os := NewMemoryObjectStore()

	kvs := []keyValue{
		kv("key-a", "value-a"),
		kv("key-b", "value-b"),
	}

	writeSST(t, os, "test.sst", kvs)

	require.Equal(t, readSST(t, os, "test.sst"), kvs)
}

func TestSimpleMerge(t *testing.T) {
	// TODO populate this
	os := NewMemoryObjectStore()

	writeSST(t, os, "a.sst", []keyValue{
		kv("a", "value-dropped"),
		kv("key-a", "value-a"),
		kv("key-d", "value-d"),
	})
	writeSST(t, os, "b.sst", []keyValue{
		kv("key-c", "value-c"),
		kv("key-f", "value-f"),
		kv("z", "value-dropped"),
	})

	m := Merger{store: os}

	require.NoError(t, m.Merge(context.Background(), "out.sst", []string{"a.sst", "b.sst"}, Span{
		Start: encodeKey("b"),
		End:   encodeKey("z"),
	}))

	require.Equal(t,
		readSST(t, os, "out.sst"),
		[]keyValue{
			kv("key-a", "value-a"),
			kv("key-c", "value-c"),
			kv("key-d", "value-d"),
			kv("key-f", "value-f"),
		})
}
