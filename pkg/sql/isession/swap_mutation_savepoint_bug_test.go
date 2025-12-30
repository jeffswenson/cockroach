// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package isession_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSwapMutationSavepointBug reproduces a bug where update_swap/delete_swap
// nodes create an internal savepoint, and when they roll back due to a
// ConditionFailedError (row doesn't exist), they fail to call Step() on the
// transaction. Combined with the internal executor's defer that restores
// readSeq to the pre-statement value, this leaves readSeq pointing to a
// sequence number that's now in the ignored range, causing the next operation
// to fail with:
//
//	"read sequence number 0 but sequence number is ignored [{0 3}] after savepoint rollback"
//
// The bug is in:
// - pkg/sql/update_swap.go:94-111
// - pkg/sql/delete_swap.go:94-111
//
// When a ConditionFailedError occurs, these nodes roll back to the savepoint
// but don't call Step() before returning. The internal executor's defer at
// pkg/sql/conn_executor_exec.go:991-995 then restores readSeq to a value
// that's now in the ignored range.
func TestSwapMutationSavepointBug(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a test table - explicitly specify a single column family
	db := s.SQLConn(t)
	_, err := db.Exec("CREATE TABLE test (id INT PRIMARY KEY, val INT)")
	require.NoError(t, err)

	_, err = db.Exec("CREATE UNIQUE INDEX test_idx ON test (val)")
	require.NoError(t, err)

	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "")
	sd.UseSwapMutations = true
	sd.BufferedWritesEnabled = false

	// Create an internal session
	idb := s.InternalDB().(descs.DB)
	session, err := idb.Session(ctx, "test-session", isql.WithSessionData(sd))
	require.NoError(t, err)
	defer session.Close(ctx)

	insertStmt, err := parser.ParseOne("INSERT INTO defaultdb.test (id, val) VALUES ($1, $2)")
	require.NoError(t, err)
	insertPrepared, err := session.Prepare(ctx, "insert", insertStmt, []*types.T{types.Int, types.Int})
	require.NoError(t, err)

	// Prepare statements - use a WHERE clause that matches ALL columns like LDR does.
	// This ensures that when the row doesn't match, swap mutation triggers ConditionFailedError.
	// Use RETURNING to match LDR's pattern (though UPDATE uses RETURNING *)
	updateStmt, err := parser.ParseOne("UPDATE defaultdb.test SET val = $1 WHERE id = $2")
	require.NoError(t, err)
	updatePrepared, err := session.Prepare(ctx, "update", updateStmt, []*types.T{types.Int, types.Int})
	require.NoError(t, err)

	// Run in a transaction to trigger the bug
	err = session.Txn(ctx, func(ctx context.Context) error {
		err := session.Savepoint(ctx, func(ctx context.Context) error {
			_, err := session.ExecutePrepared(ctx, insertPrepared, tree.Datums{
				tree.NewDInt(1), // id
				tree.NewDInt(777), // val
			})
			return err
		})
		if err != nil {
			return err
		}

		// This UPDATE will use update_swap because we enabled use_swap_mutations.
		// The row with id=999 doesn't exist, so:
		// 1. update_swap creates an internal savepoint
		// 2. The CPut fails with ConditionFailedError
		// 3. update_swap rolls back its internal savepoint (marks seqs as ignored)
		// 4. update_swap returns without calling Step()
		// 5. We return an error to roll back this outer savepoint too
		updated, err := session.ExecutePrepared(ctx, updatePrepared, tree.Datums{
			tree.NewDInt(999), // new val
			tree.NewDInt(888), // old val (doesn't matter, row doesn't exist)
		})
		if err != nil {
			return err
		}
		if updated != 0 {
			return errors.New("expected 0 rows updated")
		}
		return errors.New("abort 1337 transaction")
	})

	// The transaction should fail with the sequence number error.
	// Once the bug is fixed in update_swap.go and delete_swap.go by calling Step()
	// after savepoint rollback, this test should pass.
	require.ErrorContains(t, err, "abort 1337 transaction")
}

