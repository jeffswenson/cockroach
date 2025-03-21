package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

type sqlRowReader struct {
	selectStmt statements.Statement[tree.Statement]

	scratchDatums     []any
	primaryKeyColumns []string
	columns           []string
}

func newSQLRowReader(table catalog.TableDescriptor) (*sqlRowReader, error) {
	selectStmt, err := newSelectStatement(table)
	if err != nil {
		return nil, err
	}

	var primaryKeyColumns []string
	for _, col := range getPrimaryKeyColumns(table) {
		primaryKeyColumns = append(primaryKeyColumns, col.GetName())
	}

	var columns []string
	for _, col := range getPhysicalColumns(table) {
		columns = append(columns, col.GetName())
	}

	return &sqlRowReader{
		selectStmt:        selectStmt,
		primaryKeyColumns: primaryKeyColumns,
		columns:           columns,
	}, nil
}

func (s *sqlRowReader) ReadRow(ctx context.Context, txn isql.Txn, row cdcevent.Row) (cdcevent.Row, error) {
	var err error
	s.scratchDatums = s.scratchDatums[:0]

	s.scratchDatums, err = appendDatums(s.scratchDatums, row, s.primaryKeyColumns)
	if err != nil {
		return cdcevent.Row{}, err
	}

	datums, err := txn.QueryRowExParsed(ctx, "ldr_read_row", txn.KV(), sessiondata.NoSessionDataOverride, s.selectStmt, s.scratchDatums...)
	if err != nil {
		return cdcevent.Row{}, err
	}



}
