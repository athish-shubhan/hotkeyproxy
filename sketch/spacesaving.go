// Package sketch finds hot keys online with the Space-Saving algorithm
// (Metwally, Agrawal, El Abbadi, ICDT 2005) plus exponential decay.
//
// Space-Saving keeps k counters. A tracked key increments its counter; an
// untracked key replaces the minimum counter and inherits its count as an
// overestimate (err). Every key with true frequency above N/k is guaranteed
// to be tracked, and count-err is a lower bound on its true count, so
// "count-err >= threshold" never reports a cold key as hot. Decay halves
// every counter each half-life, so a key that cools down drops out and a
// newly viral key can take its place within a few half-lives.
package sketch

import (
	"container/heap"
	"hash/maphash"
	"sync"
	"time"
)

type entry struct {
	key   string
	count float64
	err   float64
	idx   int
}

type minHeap []*entry

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].count < h[j].count }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].idx = i; h[j].idx = j }
func (h *minHeap) Push(x any)        { e := x.(*entry); e.idx = len(*h); *h = append(*h, e) }
func (h *minHeap) Pop() any          { o := *h; e := o[len(o)-1]; *h = o[:len(o)-1]; return e }

// SpaceSaving is one sketch (not safe for concurrent use; see Sharded).
type SpaceSaving struct {
	k    int
	m    map[string]*entry
	h    minHeap
	half time.Duration
	last time.Time
}

func New(k int, halfLife time.Duration) *SpaceSaving {
	return &SpaceSaving{k: k, m: make(map[string]*entry, k), half: halfLife}
}

func (s *SpaceSaving) decay(now time.Time) {
	if s.half <= 0 {
		return
	}
	if s.last.IsZero() {
		s.last = now
		return
	}
	n := now.Sub(s.last) / s.half
	if n <= 0 {
		return
	}
	f := 1.0
	for i := time.Duration(0); i < n && i < 60; i++ {
		f /= 2
	}
	for _, e := range s.h {
		e.count *= f
		e.err *= f
	}
	s.last = s.last.Add(n * s.half)
}

// Add records w occurrences of key at time now.
func (s *SpaceSaving) Add(key []byte, w float64, now time.Time) {
	s.decay(now)
	if e, ok := s.m[string(key)]; ok {
		e.count += w
		heap.Fix(&s.h, e.idx)
		return
	}
	if len(s.h) < s.k {
		e := &entry{key: string(key), count: w}
		s.m[e.key] = e
		heap.Push(&s.h, e)
		return
	}
	min := s.h[0]
	delete(s.m, min.key)
	min.err = min.count
	min.count += w
	min.key = string(key)
	s.m[min.key] = min
	heap.Fix(&s.h, 0)
}

// Lower is a guaranteed lower bound on key's decayed count (0 if untracked).
func (s *SpaceSaving) Lower(key []byte) float64 {
	if e, ok := s.m[string(key)]; ok {
		return e.count - e.err
	}
	return 0
}

// Upper is the tracked count, an overestimate (0 if untracked).
func (s *SpaceSaving) Upper(key []byte) float64 {
	if e, ok := s.m[string(key)]; ok {
		return e.count
	}
	return 0
}

type Item struct {
	Key   string
	Count float64
	Err   float64
}

// Top returns the tracked keys whose lower bound is at least min.
func (s *SpaceSaving) Top(min float64) []Item {
	var out []Item
	for _, e := range s.h {
		if e.count-e.err >= min {
			out = append(out, Item{e.key, e.count, e.err})
		}
	}
	return out
}

// Sharded spreads keys over independent sketches by hash, each with its own
// lock, so client goroutines rarely contend. It keeps every k/n counter
// guarantee per shard.
type Sharded struct {
	seed   maphash.Seed
	shards []shard
}

type shard struct {
	mu sync.Mutex
	s  *SpaceSaving
	_  [40]byte // keep shards on separate cache lines
}

func NewSharded(n, kPerShard int, halfLife time.Duration) *Sharded {
	sh := &Sharded{seed: maphash.MakeSeed(), shards: make([]shard, n)}
	for i := range sh.shards {
		sh.shards[i].s = New(kPerShard, halfLife)
	}
	return sh
}

func (s *Sharded) pick(key []byte) *shard {
	return &s.shards[maphash.Bytes(s.seed, key)%uint64(len(s.shards))]
}

// Add records one access and returns the key's lower-bound count.
func (s *Sharded) Add(key []byte, now time.Time) float64 {
	sh := s.pick(key)
	sh.mu.Lock()
	sh.s.Add(key, 1, now)
	l := sh.s.Lower(key)
	sh.mu.Unlock()
	return l
}

func (s *Sharded) Lower(key []byte) float64 {
	sh := s.pick(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.s.Lower(key)
}

func (s *Sharded) Top(min float64) []Item {
	var out []Item
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		out = append(out, sh.s.Top(min)...)
		sh.mu.Unlock()
	}
	return out
}
