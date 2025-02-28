// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestGetCoveredSamples(t *testing.T) {
	tests := []struct {
		name     string
		span     roachpb.Span
		samples  []roachpb.Key
		expected []roachpb.Key
	}{
		{
			name: "empty samples",
			span: roachpb.Span{
				Key:    []byte("a"),
				EndKey: []byte("z"),
			},
			samples:  []roachpb.Key{},
			expected: []roachpb.Key{},
		},
		{
			name: "all samples covered",
			span: roachpb.Span{
				Key:    []byte("a"),
				EndKey: []byte("z"),
			},
			samples: []roachpb.Key{
				[]byte("b"),
				[]byte("c"),
				[]byte("d"),
			},
			expected: []roachpb.Key{
				[]byte("b"),
				[]byte("c"),
				[]byte("d"),
			},
		},
		{
			name: "some samples covered",
			span: roachpb.Span{
				Key:    []byte("a"),
				EndKey: []byte("d"),
			},
			samples: []roachpb.Key{
				[]byte("b"),
				[]byte("c"),
				[]byte("e"),
				[]byte("f"),
			},
			expected: []roachpb.Key{
				[]byte("b"),
				[]byte("c"),
			},
		},
		{
			name: "no samples covered",
			span: roachpb.Span{
				Key:    []byte("a"),
				EndKey: []byte("b"),
			},
			samples: []roachpb.Key{
				[]byte("c"),
				[]byte("d"),
				[]byte("e"),
			},
			expected: []roachpb.Key{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			covered := getCoveredSamples(tc.span, tc.samples)
			require.Equal(t, tc.expected, covered)
		})
	}
}
