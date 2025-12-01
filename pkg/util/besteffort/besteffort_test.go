package besteffort

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWarningAndError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testErr := errors.New("test error")

	operations := []struct {
		name string
		fn   func(context.Context, string, func(context.Context) error)
	}{
		{"Warning", Warning},
		{"Error", Error},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			t.Run("success", func(t *testing.T) {
				defer TestForbidSkip("success")()

				require.NotPanics(t, func() {
					op.fn(ctx, "success", func(ctx context.Context) error {
						return nil
					})
				})
			})

			t.Run("failure panics by default", func(t *testing.T) {
				defer TestForbidSkip("panic")()

				require.Panics(t, func() {
					op.fn(ctx, "panic", func(ctx context.Context) error {
						return testErr
					})
				})
			})

			t.Run("TestAllowFailure prevents panic", func(t *testing.T) {
				defer TestForbidSkip("allow")()
				defer TestAllowFailure("allow")()

				require.NotPanics(t, func() {
					op.fn(ctx, "allow", func(ctx context.Context) error {
						return testErr
					})
				})
			})

			t.Run("TestAllowFailure cleanup restores panic", func(t *testing.T) {
				defer TestForbidSkip("cleanup")()

				cleanup := TestAllowFailure("cleanup")
				require.NotPanics(t, func() {
					op.fn(ctx, "cleanup", func(ctx context.Context) error {
						return testErr
					})
				})

				cleanup()
				require.Panics(t, func() {
					op.fn(ctx, "cleanup", func(ctx context.Context) error {
						return testErr
					})
				})
			})

			t.Run("TestForbidSkip ensures execution", func(t *testing.T) {
				defer TestForbidSkip("forbid")()

				for i := 0; i < 20; i++ {
					executed := false
					op.fn(ctx, "forbid", func(ctx context.Context) error {
						executed = true
						return nil
					})
					require.True(t, executed)
				}
			})

			t.Run("skipping without TestForbidSkip", func(t *testing.T) {
				executedCount := 0
				iterations := 100

				for i := 0; i < iterations; i++ {
					op.fn(ctx, "skip", func(ctx context.Context) error {
						executedCount++
						return nil
					})
				}

				// With 50% skip probability, should execute sometimes but not always
				require.Greater(t, executedCount, 0)
				require.Less(t, executedCount, iterations)
			})
		})
	}
}
