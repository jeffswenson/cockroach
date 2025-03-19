// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestReplicationStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec":
				_, err := sqlDB.Exec(d.Input)
				if err != nil {
					return err.Error()
				}
				return "ok"
			case "show-insert":
				var tableName string
				d.ScanArgs(t, "table", &tableName)
				if tableName == "" {
					return "error: table name is required"
				}

				desc := desctestutils.TestingGetTableDescriptor(
					s.DB(),
					s.Codec(),
					"defaultdb",
					"public",
					tableName,
				)

				insertStmt, err := newInsertStatement(desc)
				require.NoError(t, err)

				stmt := insertStmt.String()

				// Test preparing the statement to ensure it is valid SQL.
				_, err = sqlDB.Exec(fmt.Sprintf("PREPARE stmt_%d AS %s", rand.Int(), stmt))
				require.NoError(t, err)

				return fmt.Sprintf("%s", stmt)
			case "show-update":
				var tableName string
				d.ScanArgs(t, "table", &tableName)
				if tableName == "" {
					return "error: table name is required"
				}

				desc := desctestutils.TestingGetTableDescriptor(
					s.DB(),
					s.Codec(),
					"defaultdb",
					"public",
					tableName,
				)

				updateStmt, err := newUpdateStatement(desc)
				require.NoError(t, err)

				stmt := updateStmt.String()

				// Test preparing the statement to ensure it is valid SQL.
				_, err = sqlDB.Exec(fmt.Sprintf("PREPARE stmt_%d AS %s", rand.Int(), stmt))
				require.NoError(t, err)

				return fmt.Sprintf("%s", stmt)
			case "show-delete":
				var tableName string
				d.ScanArgs(t, "table", &tableName)
				if tableName == "" {
					return "error: table name is required"
				}

				desc := desctestutils.TestingGetTableDescriptor(
					s.DB(),
					s.Codec(),
					"defaultdb",
					"public",
					tableName,
				)

				deleteStmt, err := newDeleteStatement(desc)
				require.NoError(t, err)

				stmt := deleteStmt.String()

				// Test preparing the statement to ensure it is valid SQL.
				_, err = sqlDB.Exec(fmt.Sprintf("PREPARE stmt_%d AS %s", rand.Int(), stmt))
				require.NoError(t, err)

				return fmt.Sprintf("%s", stmt)
			default:
				return "unknown command: " + d.Cmd
			}
		})
	})
}
