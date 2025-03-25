package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type sqlRowReader struct {
	selectStatement statements.Statement[tree.Statement]
	// keyColumnIndices is the index of the datums that are part of the primary key.
	keyColumnIndices []int
	columns          []columnSchema
}

type priorRow struct {
	row              tree.Datums
	logicalTimestamp hlc.Timestamp
	isLocal          bool
}

func newSQLRowReader(table catalog.TableDescriptor) (*sqlRowReader, error) {
	// TODO(jeffswenson): build the keyColumns from the primary key columns.
	cols := getReplicatedColumns(table)
	keyColumns := make([]int, 0, len(cols))
	for i, col := range cols {
		if col.isPrimaryKey {
			keyColumns = append(keyColumns, i)
		}
	}

	selectStatement, err := newBulkSelectStatement(table)
	if err != nil {
		return nil, err
	}

	return &sqlRowReader{
		selectStatement:  selectStatement,
		keyColumnIndices: keyColumns,
		columns:          cols,
	}, nil
}

func (r *sqlRowReader) ReadRows(
	ctx context.Context, txn isql.Txn, rows []tree.Datums,
) (map[int]priorRow, error) {
	// TODO(jeffswenson): optimize allocations. It may require a change to the
	// API.

	if len(rows) == 0 {
		return nil, nil
	}

	// Extract primary key values from the input keys and organize them by column
	pkArrays := make([][]tree.Datum, len(r.keyColumnIndices))
	for i := range pkArrays {
		pkArrays[i] = make([]tree.Datum, 0, len(rows))
	}

	index := tree.NewDArray(types.Int)
	for i := range rows {
		if err := index.Append(tree.NewDInt(tree.DInt(i))); err != nil {
			return nil, err
		}
	}

	params := make([]any, 0, len(r.keyColumnIndices)+1)
	params = append(params, index)

	for _, index := range r.keyColumnIndices {
		array := tree.NewDArray(r.columns[index].column.GetType())
		for _, row := range rows {
			if err := array.Append(row[index]); err != nil {
				return nil, err
			}
		}
		params = append(params, array)
	}

	// Execute the query using QueryBufferedEx which returns all rows at once
	rows, err := txn.QueryBufferedEx(ctx, "bulk-select", txn.KV(),
		sessiondata.NoSessionDataOverride,
		r.selectStatement.SQL,
		params...,
	)
	if err != nil {
		return nil, err
	}

	result := make(map[int]priorRow, len(rows))
	for _, row := range rows {
		if len(row) != len(r.columns)+3 {
			return nil, errors.AssertionFailedf("expected %d columns, got %d", len(r.columns)+3, len(row))
		}

		rowIndex, ok := tree.AsDInt(row[0])
		if !ok {
			return nil, errors.AssertionFailedf("expected column 0 to be the row index")
		}

		isLocal := false
		timestamp := row[1]
		if timestamp == tree.DNull {
			timestamp = row[2]
			isLocal = true
		}

		decimal, ok := tree.AsDDecimal(timestamp)
		if !ok {
			return nil, errors.AssertionFailedf("expected column 1 or 2 to be origin timestamp")
		}

		logicalTimestamp, err := hlc.DecimalToHLC(&decimal.Decimal)
		if err != nil {
			return nil, err
		}

		result[int(rowIndex)] = priorRow{
			row:              row[3:],
			logicalTimestamp: logicalTimestamp,
			isLocal:          isLocal,
		}
	}

	return result, nil
}
