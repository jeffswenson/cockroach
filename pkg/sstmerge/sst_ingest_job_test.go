package sstmerge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/stretchr/testify/require"
)

func TestSstIngestJob(t *testing.T) {
	ctx := context.Background()
	defer leaktest.AfterTest(t)()

	tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{})
	server := tc.Servers[0]
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	registry := server.JobRegistry().(*jobs.Registry)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	db := server.InternalDB().(isql.DB)

	// Create a simple string->string table.
	sqlDB.Exec(t, `
		CREATE TABLE simple (
			k STRING PRIMARY KEY,
			v STRING
		)
	`)

	// Get the table ID
	var tableID int
	sqlDB.QueryRow(t, `
		SELECT id FROM system.namespace 
		WHERE name = 'simple' AND "parentID" = database_id('defaultdb')
	`).Scan(&tableID)

	// Mark table as offline for ingestion
	setTableState(t, sqlDB, tableID, descpb.DescriptorState_OFFLINE)

	// ----------------------------------------------------------------------------
	// TODO(jeffswenson): Synthesize 10 tiny SSTs with 10 rows each for the simple table.
	// The key for each row should be <key-%d>. The value should be a random 10-char string.
	// Encode them with CRDB's index key encoding and MVCC value format.
	// ----------------------------------------------------------------------------

	// For demonstration, we'll store the SST data in memory, but you could write it
	// to ExternalStorage or local files as needed. We'll also track the row keys
	// to sample 3 of them later for splits.
	sstData := make([][]byte, 10)
	allKeys := make([]string, 0, 100) // for sampling split points

	// Hypothetical function that helps build an SST in memory.
	// Replace with whichever library you use to create ingestion-compatible SSTs.
	makeSST := func(t *testing.T, rows []kvRow) []byte {
		// e.g., create an in-memory SST file using a storage.SSTWriter or
		// ccl/importccl.backupSST (depending on your codebase).
		var buf bytes.Buffer
		writer := MakeIngestionSSTWriter(&buf)
		defer writer.Close()

		for _, row := range rows {
			if err := writer.Put(row.Key, row.Value); err != nil {
				t.Fatalf("put failed: %v", err)
			}
		}
		if err := writer.Finish(); err != nil {
			t.Fatalf("finish failed: %v", err)
		}
		return buf.Bytes()
	}

	// Helper to build a CRDB-encoded KV row using proper key encoding
	encodeRow := func(t *testing.T, keyStr, valStr string) (roachpb.Key, []byte) {
		// Use the test cluster's key codec to properly encode the table key
		key := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
		key = keys.MakeFamilyKey(key, 1)
		key = encoding.EncodeStringAscending(key, keyStr)
		return key, []byte(valStr)
	}

	// Generate 10 distinct SST files, each with 10 rows.
	for i := 0; i < 10; i++ {
		var kvRows []kvRow
		for j := 0; j < 10; j++ {
			rowIdx := i*10 + j
			keyString := fmt.Sprintf("key-%d", rowIdx)
			valString := randomString(10) // your helper to generate a 10-char random string
			k, v := encodeRow(t, keyString, valString)
			kvRows = append(kvRows, kvRow{Key: k, Value: v})
			allKeys = append(allKeys, keyString)
		}
		sstData[i] = makeSST(t, kvRows)
	}

	// ----------------------------------------------------------------------------
	// TODO(jeffswenson): Sample 3 rows written to the SST to use as split points.
	// ----------------------------------------------------------------------------

	// We'll just pick 3 random (or fixed) keys from allKeys. Let's do something simple:
	randomSplitKeys := pickNRandomDistinct(allKeys, 3)
	// Convert them to roachpb.Key for actual splits (encoded the same as above).
	var splitKeys []roachpb.Key
	for _, kStr := range randomSplitKeys {
		encoded, _ := encodeRow(t, kStr, "dummy")
		splitKeys = append(splitKeys, encoded)
	}

	details := jobspb.SstIngestDetails{
		// Hypothetical fields:
		InputSsts: []string{"nodelocal:"},
		Ssts:      make([]jobspb.SstFile, 0, 10),
		SplitKeys: make([][]byte, 0, 3),
		TableID:   uint32(tableID),
		// Add any other fields needed by your ingestion logic.
	}
	for i := range sstData {
		details.Ssts = append(details.Ssts, jobspb.SstFile{
			// Possibly store URI or actual content, depending on your codebase:
			Data: sstData[i],
		})
	}
	for _, sk := range splitKeys {
		details.InputSsts
		details.SplitKeys = append(details.SplitKeys, sk)
	}

	// Create and run the job with our details.
	job, err := jobs.TestingCreateAndStartJob(ctx, registry, db, jobs.Record{
		Description: "ingest sst test",
		Details:     details,
		Progress:    jobspb.SstIngestProgress{},
		Username:    username.TestUserName(),
	})
	require.NoError(t, err)

	jobutils.WaitForJobToSucceed(t, sqlDB, job.ID())

	// ----------------------------------------------------------------------------
	// TODO(jeffswenson): Mark the table as online (PUBLIC).
	// ----------------------------------------------------------------------------
	// Mark table as public after ingestion
	setTableState(t, sqlDB, tableID, descpb.DescriptorState_PUBLIC)

	// ----------------------------------------------------------------------------
	// TODO(jeffswenson): Scan the table and verify the contents.
	// ----------------------------------------------------------------------------
	var rowCount int
	sqlDB.QueryRow(t, `SELECT count(*) FROM simple`).Scan(&rowCount)
	require.Equal(t, 100, rowCount, "expected 100 rows ingested")

	// TODO(jeffswenson): Inspect the ranges and make sure the splits match.
}

// kvRow is just a small helper struct to package up the Key/Value for writing SSTs.
type kvRow struct {
	Key   roachpb.Key
	Value []byte
}

func randomString(n int) string {
	// Simple placeholder. Use a better random generator as needed.
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(rand.Intn(26))
	}
	return string(b)
}

func pickNRandomDistinct(keys []string, n int) []string {
	if n > len(keys) {
		n = len(keys)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return keys[:n]
}

func encodeRow(
	t *testing.T, codec keys.SQLCodec, tableID int, keyStr, valueStr string,
) storage.MVCCKeyValue {
	key := codec.TablePrefix(uint32(tableID))
	key = keys.MakeFamilyKey(key, 1)
	key = encoding.EncodeStringAscending(key, keyStr)
	return storage.MVCCKeyValue{
		Key:   storage.MVCCKey{Key: key},
		Value: []byte(valueStr),
	}
}

func makeSST(t *testing.T, rows []storage.MVCCKeyValue) []byte {
	os := NewMemoryObjectStore()
	filename := "test.sst"

	writable, err := os.Write(context.Background(), filename)
	if err != nil {
		t.Fatal(err)
	}

	options := storage.DefaultPebbleOptions().MakeWriterOptions(0, sstable.TableFormatPebblev5)
	writer := sstable.NewWriter(writable, options)

	for _, row := range rows {
		encoded := storage.EncodeMVCCKeyToBuf(nil, row.Key)
		if err := writer.Set(encoded, row.Value); err != nil {
			t.Fatal(err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.Read(context.Background(), filename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	return data
}

// setTableState updates a table's state between PUBLIC and OFFLINE.
// This is a test helper that directly modifies the descriptor state.
func setTableState(
	t *testing.T, sqlDB *sqlutils.SQLRunner, tableID int, newState descpb.DescriptorState,
) {
	t.Helper()

	stateStr := fmt.Sprintf("%q", newState.String())
	sqlDB.Exec(t, `
		WITH to_update AS (
			SELECT id, crdb_internal.pb_to_json('cockroach.sql.sqlbase.Descriptor', descriptor) AS descriptor
			FROM system.descriptor
			WHERE id = $1
		), updated AS (
			SELECT id, json_set(descriptor, ARRAY['table', 'state'], $2::JSONB) AS descriptor FROM to_update
		), encoded AS (
			SELECT id, crdb_internal.json_to_pb('cockroach.sql.sqlbase.Descriptor', descriptor) AS descriptor FROM updated
		)
		SELECT crdb_internal.unsafe_upsert_descriptor(id, descriptor, true) FROM encoded
	`, tableID, stateStr)
}
