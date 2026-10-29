package sketch

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func zipfStream(n, keys int, s float64, seed uint64) []string {
	r := rand.New(rand.NewPCG(seed, 1))
	z := rand.NewZipf(r, s, 1, uint64(keys-1))
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("k%d", z.Uint64())
	}
	return out
}

// Property: with k counters over N items, every key with true count > N/k is
// tracked, and lower <= true <= upper for every tracked key.
func TestSpaceSavingGuarantees(t *testing.T) {
	for _, skew := range []float64{1.01, 1.2, 1.5, 2.0} {
		const N, k = 200000, 100
		stream := zipfStream(N, 100000, skew, uint64(skew*100))
		exact := map[string]int{}
		ss := New(k, 0)
		now := time.Unix(0, 0)
		for _, key := range stream {
			exact[key]++
			ss.Add([]byte(key), 1, now)
		}
		for key, c := range exact {
			if float64(c) > N/k && ss.Upper([]byte(key)) == 0 {
				t.Fatalf("s=%.2f: key %s with count %d > N/k not tracked", skew, key, c)
			}
		}
		for key, e := range ss.m {
			if lo, tr := e.count-e.err, float64(exact[key]); lo > tr+1e-9 || e.count < tr-1e-9 {
				t.Fatalf("s=%.2f: key %s bounds [%v, %v] exclude true %v", skew, key, lo, e.count, tr)
			}
		}
	}
}

// After the popularity shifts, the new hot key must surface within a few
// half-lives and the old one must fall below the threshold.
func TestDecayTracksShift(t *testing.T) {
	ss := New(64, 100*time.Millisecond)
	now := time.Unix(0, 0)
	r := rand.New(rand.NewPCG(1, 2))
	step := time.Millisecond
	for i := 0; i < 2000; i++ { // 2 s with "old" at 50% of traffic
		now = now.Add(step)
		if r.Float64() < 0.5 {
			ss.Add([]byte("old"), 1, now)
		} else {
			ss.Add([]byte(fmt.Sprintf("x%d", r.IntN(5000))), 1, now)
		}
	}
	const threshold = 20
	detected := time.Duration(-1)
	start := now
	for i := 0; i < 2000; i++ {
		now = now.Add(step)
		if r.Float64() < 0.5 {
			ss.Add([]byte("new"), 1, now)
		} else {
			ss.Add([]byte(fmt.Sprintf("x%d", r.IntN(5000))), 1, now)
		}
		if detected < 0 && ss.Lower([]byte("new")) >= threshold {
			detected = now.Sub(start)
		}
	}
	if detected < 0 || detected > 200*time.Millisecond {
		t.Fatalf("new hot key detected after %v", detected)
	}
	if lo := ss.Lower([]byte("old")); lo >= threshold {
		t.Fatalf("old key still hot: %v", lo)
	}
}

func BenchmarkShardedAdd(b *testing.B) {
	s := NewSharded(16, 64, time.Second)
	keys := zipfStream(1<<16, 100000, 1.2, 9)
	now := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Add([]byte(keys[i&(1<<16-1)]), now)
			i++
		}
	})
}
