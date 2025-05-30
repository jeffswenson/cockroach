// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package prep

import (
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// Metadata encapsulates information about a statement that is gathered
// during Prepare and is later used during Describe or Execute.
type Metadata struct {
	// Note that AST may be nil if the prepared statement is empty.
	statements.Statement[tree.Statement]

	// StatementNoConstants is the statement string formatted without constants,
	// suitable for recording in statement statistics.
	StatementNoConstants string

	// StatementSummary is a summarized version of the query.
	StatementSummary string

	// Provides TypeHints and Types fields which contain placeholder typing
	// information.
	tree.PlaceholderTypesInfo

	// Columns are the types and names of the query output columns.
	Columns colinfo.ResultColumns

	// InferredTypes represents the inferred types for placeholder, using protocol
	// identifiers. Used for reporting on Describe.
	InferredTypes []oid.Oid
}

// MemoryEstimate returns an estimation (in bytes) of how much memory is used by
// the prepare metadata.
func (pm *Metadata) MemoryEstimate() int64 {
	res := int64(unsafe.Sizeof(*pm))
	res += int64(len(pm.SQL))
	// We don't have a good way of estimating the size of the AST. Just assume
	// it's a small multiple of the string length.
	res += 2 * int64(len(pm.SQL))

	res += int64(len(pm.StatementNoConstants))

	res += int64(len(pm.TypeHints)+len(pm.Types)) *
		int64(unsafe.Sizeof(tree.PlaceholderIdx(0))+unsafe.Sizeof((*types.T)(nil)))

	res += int64(len(pm.Columns)) * int64(unsafe.Sizeof(colinfo.ResultColumn{}))
	res += int64(len(pm.InferredTypes)) * int64(unsafe.Sizeof(oid.Oid(0)))

	return res
}
