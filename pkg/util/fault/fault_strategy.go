package fault

import (
	"context"
	"math"
	"math/rand/v2"
	"sync/atomic"
)

type Strategy interface {
	ShouldInject(ctx context.Context, faultName string) bool
	ModuleTestingKnobs()
}

func NewProbabilisticFaults(probability float64) *ProbabilisticFaults {
	p := &ProbabilisticFaults{}
	p.SetProbability(probability)
	return p
}

type ProbabilisticFaults struct {
	probability atomic.Uint64
}

func (p *ProbabilisticFaults) SetProbability(probability float64) {
	p.probability.Store(math.Float64bits(probability))
}

func (p *ProbabilisticFaults) ShouldInject(ctx context.Context, faultName string) bool {
	probability := math.Float64frombits(p.probability.Load())
	return rand.Float64() < probability
}

func (p *ProbabilisticFaults) ModuleTestingKnobs() {}
