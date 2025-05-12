// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// EventBuilder helps construct StreamEvent_KV events for testing.
type EventBuilder struct {
	t         *testing.T
	tableDesc catalog.TableDescriptor
	colMap    catalog.TableColMap
	codec     keys.SQLCodec
}

// newEventBuilder creates a new EventBuilder for the given table descriptor.
func newEventBuilder(t *testing.T, desc *descpb.TableDescriptor, codec keys.SQLCodec) *EventBuilder {
	tableDesc := tabledesc.NewBuilder(desc).BuildImmutableTable()
	var colMap catalog.TableColMap
	for i, col := range tableDesc.PublicColumns() {
		colMap.Set(col.GetID(), i)
	}
	return &EventBuilder{
		t:         t,
		tableDesc: tableDesc,
		colMap:    colMap,
		codec:     codec,
	}
}

// insertEvent creates an insert event for the given row at the specified timestamp.
func (b *EventBuilder) insertEvent(time hlc.Timestamp, row tree.Datums) streampb.StreamEvent_KV {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)

	event := streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   indexEntries[0].Key,
			Value: indexEntries[0].Value,
		},
	}
	event.KeyValue.Value.Timestamp = time
	event.KeyValue.Value.InitChecksum(event.KeyValue.Key)
	return event
}

// updateEvent creates an update event for the given row and previous values at the specified timestamp.
func (b *EventBuilder) updateEvent(time hlc.Timestamp, row tree.Datums, prev tree.Datums) streampb.StreamEvent_KV {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)

	event := streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key:   indexEntries[0].Key,
			Value: indexEntries[0].Value,
		},
	}
	event.KeyValue.Value.Timestamp = time

	prevEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		prev,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, prevEntries, 1)
	event.PrevValue = prevEntries[0].Value

	return event
}

// deleteEvent creates a delete event for the given row at the specified timestamp.
func (b *EventBuilder) deleteEvent(time hlc.Timestamp, row tree.Datums) streampb.StreamEvent_KV {
	indexEntries, err := rowenc.EncodePrimaryIndex(
		b.codec,
		b.tableDesc,
		b.tableDesc.GetPrimaryIndex(),
		b.colMap,
		row,
		true, // includeEmpty
	)
	require.NoError(b.t, err)
	require.Len(b.t, indexEntries, 1)

	event := streampb.StreamEvent_KV{
		KeyValue: roachpb.KeyValue{
			Key: indexEntries[0].Key,
		},
	}
	event.KeyValue.Value.Timestamp = time
	return event
}

func TestBatchHandlerReplayUniqueConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{ServerArgs: base.TestServerArgs{}})
	defer tc.Stopper().Stop(ctx)

	srv := tc.Server(0)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Create a test table with an index
	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name STRING,
			INDEX idx_name (name)
		)
	`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.dist_sender.concurrency_limit = 0`)

	// Add and remove a column to create a new primary key index that comes after the name index
	sqlDB.Exec(t, `ALTER TABLE test_table ADD COLUMN temp_col INT`)
	sqlDB.Exec(t, `ALTER TABLE test_table DROP COLUMN temp_col`)

	// Construct a kv batch handler to write to the table
	desc := cdctest.GetHydratedTableDescriptor(t, s.ExecutorConfig(), "test_table")
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	handler, err := newKVRowProcessor(
		ctx,
		&execinfra.ServerConfig{
			DB:           s.InternalDB().(descs.DB),
			LeaseManager: s.LeaseManager(),
			Settings:     s.ClusterSettings(),
		},
		&eval.Context{
			Codec:            s.Codec(),
			Settings:         s.ClusterSettings(),
			SessionDataStack: sessiondata.NewStack(sd),
		},
		sd,
		execinfrapb.LogicalReplicationWriterSpec{},
		map[descpb.ID]sqlProcessorTableConfig{
			desc.GetID(): {
				srcDesc: desc,
			},
		},
	)

	clock := s.Clock()

	require.NoError(t, err)
	defer handler.ReleaseLeases(ctx)

	eventBuilder := newEventBuilder(t, desc.TableDesc(), s.Codec())
	events := []streampb.StreamEvent_KV{
		eventBuilder.updateEvent(clock.Now(), []tree.Datum{
			tree.NewDInt(1),
			tree.NewDString("test"),
		}, []tree.Datum{
			tree.NewDInt(1),
			tree.DNull,
		}),
	}
	for i := 0; i < 3; i++ {
		_, err := handler.HandleBatch(ctx, events)
		require.NoError(t, err)
		handler.ReleaseLeases(ctx)
	}

	sqlDB.CheckQueryResults(t, `SELECT id, name FROM test_table`, [][]string{
		{"1", "test"},
	})
}
