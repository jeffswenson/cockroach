package besteffort

import (
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
)

type config struct {
	sync.Mutex
	allowedFailures map[string]struct{}
	cantSkip map[string]struct{}
}

var cfg = &config{
	allowedFailures: make(map[string]struct{}),
	cantSkip:        make(map[string]struct{}),
}

func isAllowedFailure(name string) bool {
	if !buildutil.CrdbTestBuild {
		return true
	}
	cfg.Lock()
	defer cfg.Unlock()
	_, ok := cfg.allowedFailures[name]
	return ok
}

// TestAllowFailure markes the besteffort operation with the given name as
// allowed to fail. It should be used in tests that expect the besteffort
// operation to fail silently instead of panicking.
func TestAllowFailure(name string) (cleanup func()) {
	if !buildutil.CrdbTestBuild {
		return func() {}
	}
	cfg.Lock()
	defer cfg.Unlock()
	cfg.allowedFailures[name] = struct{}{}
	return func() {
		cfg.Lock()
		defer cfg.Unlock()
		delete(cfg.allowedFailures, name)
	}
}

func shouldSkip(name string) bool {
	if !buildutil.CrdbTestBuild {
		return false
	}
	if rand.Intn(2) == 0 {
		return false
	}
	cfg.Lock()
	defer cfg.Unlock()
	_, ok := cfg.cantSkip[name]
	return !ok
}

// TestForbidSkip marks the besteffort operation with the given name as not to
// be skipped. It should be used in tests that want to ensure the besteffort
// operation is executed.
func TestForbidSkip(name string) (cleanup func()) {
	if !buildutil.CrdbTestBuild {
		return func() {}
	}
	cfg.Lock()
	defer cfg.Unlock()
	delete(cfg.cantSkip, name)
	return func() {
		cfg.Lock()
		defer cfg.Unlock()
		cfg.cantSkip[name] = struct{}{}
	}
}
