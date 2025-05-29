package randgen

import (
	"context"
	"math"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestMaxUniqueSeed(t *testing.T) {
	tests := []struct {
		typ  *types.T
		want uint64
	}{
		{types.Bool, 1},
		{types.Int2, math.MaxUint16},
		{types.Int4, math.MaxUint32},
		{types.Int, math.MaxUint64},
	}
	for _, test := range tests {
		if got := MaxUniqueSeed(test.typ); got != test.want {
			t.Errorf("MaxUniqueSeed(%s) = %d; want %d", test.typ.SQLString(), got, test.want)
		}
	}
}

func TestSeededDatum(t *testing.T) {
	tests := []struct {
		typ *types.T
	}{
		{types.Bool},
		{types.Int2},
		{types.Int4},
		{types.Int},
		{types.Date},
		{types.Timestamp},
		{types.Interval},
		{types.Uuid},
		{types.TimestampTZ},
		{types.Bytes},
		{types.MakeDecimal(1, 1)},
		{types.MakeDecimal(5, 2)},
		{types.Decimal},
		// {types.Float4},
		// {types.Float},
		// {types.String},
		// {types.INet},
		// {types.Jsonb},
		// {types.MakeBit(64)},
		// {types.Time},
		// {types.TimeTZ},
		// {types.MakeCollatedString(types.String, "en")},
		// {types.Oid},
		// {types.IntArray},
		// {types.StringArray},
		// {types.MakeEnum(15210, 15213)},
		// {types.TSVector},
		// {types.TSQuery},
		// {types.PGVector},
	}
	for _, test := range tests {
		t.Run(test.typ.PGName(), func(t *testing.T) {
			maxSeed := MaxUniqueSeed(test.typ)
			ctx := context.Background()
			evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(ctx)

			randomSeed := func() uint64 {
				seed := rand.Uint64()
				if maxSeed == math.MaxUint64 {
					return seed
				}
				return seed % (maxSeed + 1)
			}

			for range 10_000 {
				seedA := randomSeed()
				seedB := randomSeed()

				cmp, err := SeededDatum(test.typ, seedA).Compare(ctx, evalCtx, SeededDatum(test.typ, seedA))
				require.NoError(t, err, "error comparing seeded datums for type %s with seed %d", test.typ.PGName(), seedA)
				require.Zero(t, cmp, "seed %d should produce a deterministic value for type %s", seedA, test.typ.PGName())
				if seedA != seedB {
					cmp, err = SeededDatum(test.typ, seedA).Compare(ctx, evalCtx, SeededDatum(test.typ, seedB))
					require.NoError(t, err, "error comparing seeded datums for type %s with seeds %d and %d", test.typ.PGName(), seedA, seedB)
					require.NotZero(t, cmp, "seeds %d and %d should produce different values for type %s", seedA, seedB, test.typ.PGName())
				}
			}
		})
	}
}
