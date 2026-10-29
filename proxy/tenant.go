package proxy

// Tenant isolation, a simplified version of ABase's scheme:
//
//   - Request units (RU). Every request is charged after it completes: a
//     read that went to Redis costs 1 RU per started 4 KiB of reply, a write
//     1 RU per started 1 KiB of request, and a read served from the proxy
//     cache 0.25 RU (it still costs proxy CPU, but no shard work).
//   - Layer 1, per tenant: an RU/s quota as a token bucket (burst = one
//     second of quota). By default a request that finds the bucket empty is
//     not rejected; it is scheduled at a tenth of its tenant's weight, so a
//     tenant over quota gets only what the others leave unused (soft). With
//     QuotaShape the quota is enforced: the request waits for tokens on its
//     own connection, or is refused if the wait would exceed MaxQuotaWait.
//   - Layer 2, per shard: weighted fair queueing in front of each Redis
//     shard. At most MaxInFlight requests per shard are outstanding (Redis
//     executes one connection's commands in order, so anything already sent
//     cannot be reordered); the rest wait in one queue ordered by virtual
//     finish time, finish = max(virtual time, tenant's last finish on this
//     shard) + estimated RU / weight. Estimates are a per-tenant,
//     per-command moving average of observed RU, so a tenant sending heavy
//     reads pays for them in queue position, not just in its bill.
//
// The tenant of a connection is set with AUTH <tenant> <password> (or
// AUTH <tenant>); the password is not checked.

import (
	"container/heap"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hotkeyproxy/backend"
)

type TenantConfig struct {
	Name     string
	QuotaRU  float64 // RU per second; 0 = unlimited
	Weight   float64
	Priority int
}

type Tenant struct {
	cfg TenantConfig

	mu         sync.Mutex
	tokens     float64
	last       time.Time
	lastFinish []float64          // per shard
	est        map[string]float64 // moving average RU per command

	Requests  atomic.Int64
	RUMilli   atomic.Int64 // RU x 1000
	OverQuota atomic.Int64
	CacheHits atomic.Int64
	Throttled atomic.Int64 // shaping: requests delayed until tokens were available
	Refused   atomic.Int64 // shaping: requests refused (would have waited too long)
}

func newTenant(c TenantConfig, shards int) *Tenant {
	if c.Weight <= 0 {
		c.Weight = 1
	}
	return &Tenant{cfg: c, tokens: c.QuotaRU, last: time.Now(), lastFinish: make([]float64, shards), est: map[string]float64{}}
}

// admit refills the bucket and takes cost; false means over quota.
func (t *Tenant) admitLocked(cost float64, now time.Time) bool {
	if t.cfg.QuotaRU <= 0 {
		return true
	}
	t.tokens = math.Min(t.cfg.QuotaRU, t.tokens+t.cfg.QuotaRU*now.Sub(t.last).Seconds())
	t.last = now
	if t.tokens >= cost {
		t.tokens -= cost
		return true
	}
	return false
}

// reserve takes cost from the bucket, letting it go into debt: the request
// may proceed after wait (the time to earn the debt back at the quota rate).
// A request that would wait longer than maxWait is refused and takes nothing.
func (t *Tenant) reserve(cost float64, now time.Time, maxWait time.Duration) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tokens = math.Min(t.cfg.QuotaRU, t.tokens+t.cfg.QuotaRU*now.Sub(t.last).Seconds())
	t.last = now
	left := t.tokens - cost
	var wait time.Duration
	if left < 0 {
		wait = time.Duration(-left / t.cfg.QuotaRU * float64(time.Second))
		if wait > maxWait {
			return 0, false
		}
	}
	t.tokens = left
	return wait, true
}

func (t *Tenant) estimate(cmd string, reqLen int) float64 {
	if writeCmds[cmd] {
		return ruWrite(reqLen)
	}
	t.mu.Lock()
	e, ok := t.est[cmd]
	t.mu.Unlock()
	if !ok {
		return 1
	}
	return e
}

func (t *Tenant) charge(cmd string, ru float64, write bool) {
	t.RUMilli.Add(int64(ru * 1000))
	if !write {
		t.mu.Lock()
		if e, ok := t.est[cmd]; ok {
			t.est[cmd] = 0.9*e + 0.1*ru
		} else {
			t.est[cmd] = ru
		}
		t.mu.Unlock()
	}
}

var writeCmds = map[string]bool{"SET": true, "INCR": true, "INCRBY": true, "DECR": true, "DECRBY": true, "ZINCRBY": true,
	"ZADD": true, "HSET": true, "HINCRBY": true, "DEL": true, "EXPIRE": true, "LPUSH": true, "RPUSH": true, "MSET": true, "UNLINK": true}

func ruRead(replyLen int) float64      { return math.Max(1, math.Ceil(float64(replyLen)/4096)) }
func ruWrite(reqLen int) float64       { return math.Max(1, math.Ceil(float64(reqLen)/1024)) }
func ruHit() float64                   { return 0.25 }
func tenantName(s string) string       { return strings.TrimSpace(s) }
func (t *Tenant) Name() string         { return t.cfg.Name }
func (t *Tenant) Config() TenantConfig { return t.cfg }

// ---------------------------------------------------------------- WFQ

type fqItem struct {
	finish, start float64
	seq           uint64
	enc           []byte
	out           chan backend.Result
}

type fqHeap []*fqItem

func (h fqHeap) Len() int { return len(h) }
func (h fqHeap) Less(i, j int) bool {
	if h[i].finish != h[j].finish {
		return h[i].finish < h[j].finish
	}
	return h[i].seq < h[j].seq
}
func (h fqHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *fqHeap) Push(x interface{}) { *h = append(*h, x.(*fqItem)) }
func (h *fqHeap) Pop() interface{} {
	old := *h
	it := old[len(old)-1]
	*h = old[:len(old)-1]
	return it
}

type wfq struct {
	srv      *backend.Server
	shard    int
	max      int
	mu       sync.Mutex
	inflight int
	vtime    float64
	seq      uint64
	h        fqHeap
	Queued   atomic.Int64
}

func newWFQ(srv *backend.Server, shard, max int) *wfq { return &wfq{srv: srv, shard: shard, max: max} }

func (q *wfq) submit(t *Tenant, cost float64, enc []byte) <-chan backend.Result {
	now := time.Now()
	t.mu.Lock()
	w := t.cfg.Weight
	if !t.admitLocked(cost, now) {
		w *= 0.1
		t.OverQuota.Add(1)
	}
	t.mu.Unlock()
	return q.enqueue(t, cost, enc, w)
}

// enqueue places a request in the shard's fair queue at weight w.
func (q *wfq) enqueue(t *Tenant, cost float64, enc []byte, w float64) <-chan backend.Result {
	out := make(chan backend.Result, 1)
	q.mu.Lock()
	start := math.Max(q.vtime, t.lastFinishAt(q.shard))
	it := &fqItem{start: start, finish: start + cost/w, enc: enc, out: out}
	t.setLastFinish(q.shard, it.finish)
	q.seq++
	it.seq = q.seq
	if q.inflight < q.max && len(q.h) == 0 {
		q.inflight++
		q.vtime = start
		q.mu.Unlock()
		q.dispatch(it)
		return out
	}
	heap.Push(&q.h, it)
	q.Queued.Add(1)
	q.mu.Unlock()
	return out
}

func (t *Tenant) lastFinishAt(shard int) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastFinish[shard]
}

func (t *Tenant) setLastFinish(shard int, f float64) {
	t.mu.Lock()
	t.lastFinish[shard] = f
	t.mu.Unlock()
}

func (q *wfq) dispatch(it *fqItem) {
	ch := q.srv.Go(it.enc)
	go func() {
		r := <-ch
		it.out <- r
		q.mu.Lock()
		q.inflight--
		if len(q.h) > 0 {
			nx := heap.Pop(&q.h).(*fqItem)
			q.vtime = math.Max(q.vtime, nx.start)
			q.inflight++
			q.mu.Unlock()
			q.dispatch(nx)
			return
		}
		q.mu.Unlock()
	}()
}

// ---------------------------------------------------------------- stats

type TenantStats struct {
	Name      string  `json:"name"`
	QuotaRU   float64 `json:"quota_ru_per_sec"`
	Weight    float64 `json:"weight"`
	Requests  int64   `json:"requests"`
	RU        float64 `json:"ru"`
	OverQuota int64   `json:"over_quota_requests"`
	CacheHits int64   `json:"cache_hits"`
	Throttled int64   `json:"throttled_requests"`
	Refused   int64   `json:"refused_requests"`
}
