// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"math"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// MaxUniqueSeed returns the largest seed that still maps to a unique datum for
// the supplied type.  All seeds in the inclusive range [0, MaxUniqueSeed(typ)]
// are guaranteed to produce distinct values when passed to SeededDatum.
//
// 0 indicates that the type is not supported.
func MaxUniqueSeed(typ *types.T) uint64 {
	width := func(bitWidth int32) uint64 {
		if bitWidth <= 0 {
			return 0 // For 0 bits, only seed 0 is valid, representing 1 value. Max seed is (1<<0)-1 = 0.
		}
		// If bitWidth is 64 or more, it covers the entire uint64 range.
		// Note: typ.Width() for PGVector dimensions can lead to 8*dim > 64.
		// In such cases, we are still limited by the uint64 seed.
		if bitWidth >= 64 {
			return math.MaxUint64
		}
		// For bitWidth between 1 and 63 (inclusive).
		return (uint64(1) << bitWidth) - 1
	}

	switch typ.Family() {
	case types.BoolFamily:
		return 1
	case types.IntFamily:
		return width(typ.Width())
	case types.DateFamily:
		return uint64(pgdate.HighDate.PGEpochDays() - pgdate.LowDate.PGEpochDays())
	case types.IntervalFamily:
		return math.MaxUint64
	case types.UuidFamily:
		return math.MaxUint64
	case types.TimestampTZFamily:
		return math.MaxUint64
	case types.BytesFamily:
		return math.MaxUint64
	case types.DecimalFamily:
		if typ.Precision() == 0 {
			return math.MaxUint64
		}
		return uint64(math.Pow10(int(typ.Precision())))
	default:
		return 0
	}
}

// SeededDatum deterministically converts a seed into a datum of the requested
// type. If seed <= MaxUniqueSeed(typ), the resulting datum is unique for that
// type.
func SeededDatum(typ *types.T, seed uint64) tree.Datum {
	// take is a helper that an integer < `max`, then returns the remainder of the seed.
	// If take is called multiple times, the sum(max1, max2, ...) will equal the max unique seed for the type.
	take := func(seed uint64, max uint64) (rest uint64, value uint64) {
		value = seed % max
		rest = seed / max
		return rest, value
	}
	switch typ.Family() {
	case types.BoolFamily:
		return tree.MakeDBool(seed%2 == 0)
	case types.IntFamily:
		return tree.NewDInt(tree.DInt(seed))
	case types.DateFamily:
		epoch := pgdate.LowDate.PGEpochDays() + int32(seed%MaxUniqueSeed(typ))
		d, err := pgdate.MakeDateFromPGEpoch(epoch)
		if err != nil {
			panic(errors.Wrapf(err, "error making date from epoch days %d (derived from seed %d)", epoch, seed))
		}
		return tree.NewDDate(d)
	case types.TimestampFamily:
		seconds, nanoSeconds := take(seed, 1e9)
		t := time.Unix(int64(seconds), int64(nanoSeconds))
		d, err := tree.MakeDTimestamp(t, time.Microsecond)
		if err != nil {
			panic(errors.Wrapf(err, "error making timestamp from seed %d", seed))
		}
		return d
	case types.IntervalFamily:
		d := duration.FromInt64(int64(seed))
		return tree.NewDInterval(d, types.DefaultIntervalTypeMetadata)
	case types.UuidFamily:
		var u uuid.UUID
		for i := 0; i < 16; i++ {
			var b uint64
			seed, b = take(seed, math.MaxUint / 16)
			u[i] = byte(b)
		}
		return tree.NewDUuid(tree.DUuid{UUID: u})
	case types.TimestampTZFamily:
		seconds, nanoSeconds := take(seed, 1e9)
		t := time.Unix(int64(seconds), int64(nanoSeconds))
		d, err := tree.MakeDTimestampTZ(t, time.Microsecond)
		if err != nil {
			panic(errors.Wrapf(err, "error making timestamp from seed %d", seed))
		}
		return d
	case types.BytesFamily:
		bytes := make([]byte, math.MaxUint64 / math.MaxUint8)
		for i := range bytes {
			var b uint64
			seed, b = take(seed, math.MaxUint8)
			bytes[i] = byte(b)
		}
		return tree.NewDBytes(tree.DBytes(bytes))
	case types.DecimalFamily:
		var result apd.Decimal
		if typ.Precision() == 0 {

		} else {
			return tree.NewDDecimal(tree.DDecimal{Decimal: apd.Zero})
		}
		decimal := apd.New(int(typ.Precision()), int(typ.Scale()))
		decimal.SetInt64(int64(seed))
		return tree.NewDDecimal(tree.DDecimal{Decimal: *decimal})
	default:
		panic("not implemented")
	}
}
