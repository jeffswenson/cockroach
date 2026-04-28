// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	workloadrand "github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var diffFlags = pflag.NewFlagSet(`diff`, pflag.ContinueOnError)
var diffLimit = diffFlags.Int("limit", 100,
	"Maximum number of row diffs to return")

func init() {
	AddSubCmd(func(_ bool) *cobra.Command {
		diffCmd := SetCmdDefaults(&cobra.Command{
			Use:   `diff <table-a> <pgurl-a> <table-b> <pgurl-b>`,
			Short: `compare two tables across two database connections`,
			Long: `Compare two tables across two database connections and print row-level
differences. Both tables must have the same schema. Table names can be
qualified (e.g. db.schema.table). Each pgurl should be a PostgreSQL
connection string.`,
			Args: cobra.ExactArgs(4),
		})
		diffCmd.Flags().AddFlagSet(diffFlags)
		diffCmd.Run = HandleErrs(runDiff)
		return diffCmd
	})
}

func runDiff(_ *cobra.Command, args []string) error {
	tableA, urlA := args[0], args[1]
	tableB, urlB := args[2], args[3]

	connA, err := gosql.Open("cockroach", urlA)
	if err != nil {
		return err
	}
	defer connA.Close()

	connB, err := gosql.Open("cockroach", urlB)
	if err != nil {
		return err
	}
	defer connB.Close()

	pingTimeout := 10 * time.Second
	pingCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := connA.PingContext(pingCtx); err != nil {
		return fmt.Errorf("connecting to table-a database: %w", err)
	}
	if err := connB.PingContext(pingCtx); err != nil {
		return fmt.Errorf("connecting to table-b database: %w", err)
	}

	diffs, err := workloadrand.Diff(connA, tableA, connB, tableB, *diffLimit)
	if err != nil {
		return err
	}

	for _, d := range diffs {
		fmt.Println(d.String())
	}

	fmt.Printf("\n%d diff(s) found", len(diffs))
	if len(diffs) >= *diffLimit {
		fmt.Printf(" (limited to %d)", *diffLimit)
	}
	fmt.Println()

	return nil
}
