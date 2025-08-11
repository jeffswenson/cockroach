package backup

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestFlakyStorage tests backup and restore operations with flaky storage enabled.
// It creates a table with 100 rows, performs a detached backup with retries,
// waits for completion, drops the table, performs a detached restore with retries,
// waits for completion, and verifies the restored data matches the original.
func TestFlakyStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	//strategy := fault.NewProbabilisticFaults(0.01)
	strategy := fault.NewProbabilisticFaults(0.01)

	// Set up a slim test server with flaky storage enabled
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs:    jobs.NewTestingKnobsWithShortIntervals(),
				FaultInjectionKnobs: strategy,
			},
		},
	}

	ctx := context.Background()

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, 0, InitManualReplication, params)
	defer cleanupFn()

	sql := sqlutils.MakeSQLRunner(tc.Conns[0])

	nextRow := 1
	writeRows := func(t *testing.T, sql *sqlutils.SQLRunner) {
		for i := 0; i < 10; i++ {
			sql.Exec(t, fmt.Sprintf(`INSERT INTO testdb.test_table VALUES (%d, 'row_%d', %d)`, nextRow, nextRow, nextRow*10))
			nextRow++
		}
	}

	sql.Exec(t, `SET CLUSTER SETTING backup.compaction.threshold = 5`)
	sql.Exec(t, `SET CLUSTER SETTING backup.compaction.window_size = 3`)

	// Create a test table with 100 rows
	sql.Exec(t, `SET CLUSTER SETTING rpc.experimental_drpc.enabled = false`)
	sql.Exec(t, `CREATE DATABASE testdb`)
	sql.Exec(t, `CREATE TABLE testdb.test_table (id INT PRIMARY KEY, name STRING, value INT)`)

	writeRows(t, sql)
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO 'nodelocal://1/backup'`)
		return err
	})

	for i := 0; i < 10; i++ {
		writeRows(t, sql)
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO LATEST IN 'nodelocal://1/backup'`)
			return err
		})
	}

	originalFingerprint := sql.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)

	sqlDB.Exec(t, `DROP TABLE testdb.test_table`)

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `RESTORE TABLE testdb.test_table FROM LATEST IN 'nodelocal://1/backup'`)
		return err
	})

	restoredFingerprint := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)
	require.Equal(t, originalFingerprint, restoredFingerprint, "Fingerprints should match")
}

// TestFlakyStorage tests backup and restore operations with flaky storage enabled.
// It creates a table with 100 rows, performs a detached backup with retries,
// waits for completion, drops the table, performs a detached restore with retries,
// waits for completion, and verifies the restored data matches the original.
func TestFlakyStorageCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	strategy := fault.NewProbabilisticFaults(0)

	// Set up a slim test server with flaky storage enabled
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs:    jobs.NewTestingKnobsWithShortIntervals(),
				FaultInjectionKnobs: strategy,
			},
		},
	}

	ctx := context.Background()

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, 0, InitManualReplication, params)
	defer cleanupFn()

	sql := sqlutils.MakeSQLRunner(tc.Conns[0])

	nextRow := 1
	writeRows := func(t *testing.T, sql *sqlutils.SQLRunner) {
		for i := 0; i < 10; i++ {
			sql.Exec(t, fmt.Sprintf(`INSERT INTO testdb.test_table VALUES (%d, 'row_%d', %d)`, nextRow, nextRow, nextRow*10))
			nextRow++
		}
	}

	sql.Exec(t, `SET CLUSTER SETTING backup.compaction.threshold = 5`)
	sql.Exec(t, `SET CLUSTER SETTING backup.compaction.window_size = 3`)

	// Create a test table with 100 rows
	sql.Exec(t, `SET CLUSTER SETTING rpc.experimental_drpc.enabled = false`)
	sql.Exec(t, `CREATE DATABASE testdb`)
	sql.Exec(t, `CREATE TABLE testdb.test_table (id INT PRIMARY KEY, name STRING, value INT)`)

	writeRows(t, sql)
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO 'nodelocal://1/backup'`)
		return err
	})

	for i := 0; i < 10; i++ {
		writeRows(t, sql)
		testutils.SucceedsSoon(t, func() error {
			_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO LATEST IN 'nodelocal://1/backup'`)
			return err
		})
	}

	originalFingerprint := sql.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)

	strategy.SetProbability(0.01)
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO LATEST IN 'nodelocal://1/backup'`)
		if err != nil {
			return fmt.Errorf("failed to backup with flaky storage: %w", err)
		}
		return backuptestutils.CompactLatestChain(ctx, tc.Conns[0], "nodelocal://1/backup")
	})

	sqlDB.Exec(t, `DROP TABLE testdb.test_table`)

	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.DB.ExecContext(ctx, `RESTORE TABLE testdb.test_table FROM LATEST IN 'nodelocal://1/backup'`)
		return err
	})

	restoredFingerprint := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)
	require.Equal(t, originalFingerprint, restoredFingerprint, "Fingerprints should match")
}
