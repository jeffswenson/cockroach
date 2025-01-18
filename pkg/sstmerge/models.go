package sstmerge

import "bytes"

type Span struct {
	Start []byte
	End   []byte
}

func (s Span) Overlaps(other Span) bool {
	return 0 <= bytes.Compare(s.End, other.Start) && 0 <= bytes.Compare(s.Start, other.End)
}

type SstMetadata struct {
	Span     Span
	Filename string
}

type MergedSpan struct {
	id           taskId
	Span         Span
	SstMetadatas []SstMetadata
}
