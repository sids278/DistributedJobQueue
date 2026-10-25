package queue

import (
	"math/rand"
	"time"
)

// Backoff computes retry delays: Base * 2^(attempt-1), capped at Max, with up
// to Jitter (0..1) of the delay randomly subtracted so that jobs failing
// together do not retry in lockstep.
type Backoff struct {
	Base   time.Duration
	Max    time.Duration
	Jitter float64
}

// DefaultBackoff gives delays of roughly 1s, 2s, 4s, ... up to 5m.
var DefaultBackoff = Backoff{Base: time.Second, Max: 5 * time.Minute, Jitter: 0.2}

// Delay returns the wait before retrying after the given failed attempt (1-based).
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt && d < b.Max; i++ {
		d *= 2
	}
	if b.Max > 0 && d > b.Max {
		d = b.Max
	}
	if j := b.Jitter; j > 0 {
		if j > 1 {
			j = 1
		}
		d -= time.Duration(rand.Float64() * j * float64(d))
	}
	return d
}
