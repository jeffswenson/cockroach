package besteffort

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func Warning(
	ctx context.Context,
	name string,
	do func(ctx context.Context) error) {
	if shouldSkip(name) {
		log.Dev.Infof(ctx, "skipping best effort operation '%s'", name)
		return
	}
	err := do(ctx)
	if err != nil {
		if !isAllowedFailure(name) {
			panic(err)
		}
		log.Dev.Warningf(ctx, "best effort operation '%s' failed: %+v", name, err)
	}
}

func Error(
	ctx context.Context,
	name string,
	do func(ctx context.Context) error) {
	if shouldSkip(name) {
		log.Dev.Infof(ctx, "skipping best effort operation '%s'", name)
		return
	}
	err := do(ctx)
	if err != nil {
		if !isAllowedFailure(name) {
			panic(err)
		}
		log.Dev.Errorf(ctx, "best effort operation '%s' failed: %+v", name, err)
	}
}
