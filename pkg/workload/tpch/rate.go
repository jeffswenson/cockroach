package tpch

import "time"

func Limit(maxBuffered int, rate float64) chan struct{} {
	result := make(chan struct{}, maxBuffered)
	go func() {
		for range time.Tick(time.Duration(float64(time.Second) / rate)) {
			result <- struct{}{}
		}
	}()
	return result
}
