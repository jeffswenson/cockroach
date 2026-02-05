// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goroutinedump

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// CaptureDump captures the current goroutine stack traces and saves them to a file.
// The file is saved to the specified directory (typically the test's log directory).
//
// The namePrefix is used to create a descriptive filename along with a timestamp.
// This function logs the file location or any errors to the test log.
func CaptureDump(t *testing.T, dir, namePrefix string) {
	dump := allstacks.Get()

	filename := fmt.Sprintf("%s_goroutine_dump_%s.txt",
		namePrefix, timeutil.Now().Format("20060102_150405"))

	path := filepath.Join(dir, filename)

	if err := os.WriteFile(path, dump, 0644); err != nil {
		t.Logf("Failed to write goroutine dump: %v", err)
	} else {
		t.Logf("Wrote goroutine dump to %s", path)
	}
}
