package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

// TODO(jeffswenson): create a data driven test that prints out the SQL queries
// used by LDR for a table.

// TestDataDriven runs datadriven test to show the SQL generated for LDR
// replication queries.
//   - exec-sql: executes the input SQL statement
//   - show-queries: shows queries for the specified table (stub for now)
func TestDataDrivenCrudQueries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		// Start a test server
		srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(sqlDB)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				// Execute the input SQL statement
				tdb.Exec(t, d.Input)
				return ""

			case "show-queries":
				// For now, just echo the table name
				var tableName string
				d.ScanArgs(t, "table", &tableName)
				return "Showing queries for table: " + tableName

			default:
				t.Fatalf("unknown command %s", d.Cmd)
				return ""
			}
		})
	})
}
