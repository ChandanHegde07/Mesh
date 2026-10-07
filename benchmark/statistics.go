package benchmark

import (
	"math"
	"sort"
	"time"
)

type Distribution struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min_ms"`
	Mean   float64 `json:"mean_ms"`
	Median float64 `json:"median_ms"`
	StdDev float64 `json:"stddev_ms"`
	P95    float64 `json:"p95_ms"`
	P99    float64 `json:"p99_ms"`
	Max    float64 `json:"max_ms"`
}

func DistributionOf(ds []time.Duration) Distribution {
	if len(ds) == 0 {
		return Distribution{}
	}
	xs := make([]float64, len(ds))
	var sum float64
	for i, d := range ds {
		xs[i] = float64(d) / float64(time.Millisecond)
		sum += xs[i]
	}
	sort.Float64s(xs)
	mean := sum / float64(len(xs))
	var sq float64
	for _, x := range xs {
		sq += (x - mean) * (x - mean)
	}
	p := func(q float64) float64 {
		i := int(math.Ceil(q*float64(len(xs)))) - 1
		if i < 0 {
			i = 0
		}
		return xs[i]
	}
	return Distribution{len(xs), xs[0], mean, p(.5), math.Sqrt(sq / float64(len(xs))), p(.95), p(.99), xs[len(xs)-1]}
}
