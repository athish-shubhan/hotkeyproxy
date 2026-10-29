// Package proxy is a RESP proxy in front of Redis Cluster that detects hot
// keys online and serves them without melting the shard that owns them,
// reproducing the proxy-layer ideas of ByteDance's ABase (SIGMOD '25):
//
//   - hot-key detection with a decaying Space-Saving sketch;
//   - an active-update cache for hot reads: entries are refreshed in the
//     background before they go stale, so a hot key never takes a miss
//     storm, and a reply older than the staleness bound is never served;
//   - leases on first fill: concurrent misses of a hot key make one backend
//     request (the memcache lease idea, against thundering herds);
//   - write combining for hot counters: INCRBY/ZINCRBY on a hot key within a
//     short window go to Redis as one command, and every caller still gets
//     the exact value its increment produced in the combined order.
//
// Limited fan-out hashing (which proxies a key may be sent to) lives in the
// client and load generator, because it is a property of routing to the
// proxy fleet, not of one proxy.
package proxy

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hotkeyproxy/backend"
	"hotkeyproxy/cluster"
	"hotkeyproxy/resp"
	"hotkeyproxy/sketch"
)

type Config struct {
	Listen        string
	StatsAddr     string
	Seed          string // any cluster node
	ConnsPerShard int
	Hot           bool          // enable hot-key detection, cache, leases and combining
	HotThreshold  float64       // decayed accesses (lower bound) for a key to count as hot
	HalfLife      time.Duration // sketch decay
	Staleness     time.Duration // never serve a cached reply older than this
	CacheEntries  int
	CombineWindow time.Duration // 0 disables write combining
	Sample        int           // record 1 of Sample accesses in the sketch (1 = all)

	Tenants     []TenantConfig // known tenants (others get weight 1, no quota)
	Isolation   bool           // RU-weighted fair queueing in front of each shard (tenant.go)
	MaxInFlight int            // isolation: outstanding requests per shard
	// QuotaShape enforces RU quotas instead of only deprioritizing: a request
	// over its tenant's quota waits for tokens (backpressure on that
	// connection), and is refused if it would wait longer than MaxQuotaWait.
	QuotaShape   bool
	MaxQuotaWait time.Duration
}

func DefaultConfig() Config {
	return Config{Listen: "127.0.0.1:7100", StatsAddr: "127.0.0.1:7200", Seed: "127.0.0.1:7001", ConnsPerShard: 4,
		Hot: true, HotThreshold: 200, HalfLife: time.Second, Staleness: 50 * time.Millisecond, CacheEntries: 4096,
		CombineWindow: 0, Sample: 1, MaxInFlight: 16}
}

type Stats struct {
	Requests        atomic.Int64
	CacheHits       atomic.Int64
	LeaseWaits      atomic.Int64 // requests that waited on another request's fill instead of going to Redis
	Fills           atomic.Int64
	Refreshes       atomic.Int64
	StaleBypass     atomic.Int64 // cached entry older than the bound: forwarded instead
	CombinedOps     atomic.Int64
	CombinedBatches atomic.Int64
	staleHist       [10]atomic.Int64 // age of served cached replies
}

var staleBuckets = []time.Duration{time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
	20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, time.Second}

func (s *Stats) observeAge(a time.Duration) {
	i := sort.Search(len(staleBuckets), func(i int) bool { return a <= staleBuckets[i] })
	s.staleHist[i].Add(1)
}

type Proxy struct {
	cfg     Config
	slots   *cluster.Map
	servers []*backend.Server
	sk      *sketch.Sharded
	cache   *cache
	comb    *combiner
	S       Stats
	sampleN atomic.Uint64

	tmu     sync.Mutex
	tenants map[string]*Tenant
	fq      []*wfq
}

// tenant returns (creating if needed) the tenant called name.
func (p *Proxy) tenant(name string) *Tenant {
	if name == "" {
		name = "default"
	}
	p.tmu.Lock()
	defer p.tmu.Unlock()
	t := p.tenants[name]
	if t == nil {
		c := TenantConfig{Name: name, Weight: 1}
		for _, tc := range p.cfg.Tenants {
			if tc.Name == name {
				c = tc
			}
		}
		t = newTenant(c, len(p.servers))
		p.tenants[name] = t
	}
	return t
}

func New(cfg Config) (*Proxy, error) {
	seed := backend.NewServer(cfg.Seed, 1)
	rep, err := seed.Do(resp.AppendCommand(nil, []byte("CLUSTER"), []byte("SLOTS")))
	if err != nil {
		return nil, err
	}
	m, err := cluster.FromClusterSlots(rep)
	if err != nil {
		return nil, err
	}
	p := &Proxy{cfg: cfg, slots: m}
	for _, a := range m.Addrs {
		p.servers = append(p.servers, backend.NewServer(a, cfg.ConnsPerShard))
	}
	p.tenants = map[string]*Tenant{}
	for i, sv := range p.servers {
		p.fq = append(p.fq, newWFQ(sv, i, max(cfg.MaxInFlight, 1)))
	}
	p.sk = sketch.NewSharded(16, 64, cfg.HalfLife)
	p.cache = newCache(p, cfg.CacheEntries)
	p.comb = &combiner{p: p, batches: map[string]*batch{}}
	if cfg.Hot {
		go p.cache.refreshLoop()
	}
	return p, nil
}

func (p *Proxy) Shards() []*backend.Server { return p.servers }

func (p *Proxy) ListenAndServe() error {
	if p.cfg.StatsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/", p.statsHandler)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		go http.ListenAndServe(p.cfg.StatsAddr, mux)
	}
	ln, err := net.Listen("tcp", p.cfg.Listen)
	if err != nil {
		return err
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		go p.serve(c)
	}
}

// reply is a future: either ready bytes or a backend result to convert.
type reply struct {
	raw []byte
	ch  <-chan backend.Result
	fn  func() []byte
	// RU accounting for a forwarded command, charged when the reply is
	// written (no closure per request)
	acct   *Tenant
	cmd    string
	write  bool
	reqLen int
}

func (r reply) wait() []byte {
	if r.fn != nil {
		return r.fn()
	}
	if r.ch != nil {
		res := <-r.ch
		if res.Err != nil {
			return resp.AppendError(nil, "ERR proxy: "+res.Err.Error())
		}
		if r.acct != nil {
			if r.write {
				r.acct.charge(r.cmd, ruWrite(r.reqLen), true)
			} else {
				r.acct.charge(r.cmd, ruRead(len(res.Reply.Raw)), false)
			}
		}
		return res.Reply.Raw
	}
	return r.raw
}

type connState struct {
	noCache bool
	tenant  *Tenant
}

func (p *Proxy) serve(nc net.Conn) {
	defer nc.Close()
	r := resp.NewReader(nc)
	out := make(chan reply, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		w := bufio.NewWriterSize(nc, 64<<10)
		for rp := range out {
			if _, err := w.Write(rp.wait()); err != nil {
				for range out {
				}
				return
			}
			if len(out) == 0 {
				if w.Flush() != nil {
					for range out {
					}
					return
				}
			}
		}
		w.Flush()
	}()
	st := connState{tenant: p.tenant("")}
	for {
		args, err := r.ReadCommand()
		if err != nil || len(args) == 0 {
			break
		}
		if strings.EqualFold(string(args[0]), "QUIT") {
			out <- reply{raw: resp.AppendSimple(nil, "OK")}
			break
		}
		out <- p.handle(args, &st)
	}
	close(out)
	<-done
}

var cacheable = map[string]bool{"GET": true, "ZREVRANGE": true, "ZRANGE": true, "ZSCORE": true, "HGET": true, "ZCARD": true, "LRANGE": true}

// cmdNames interns command names in both cases, so the hot path gets a
// constant string without allocating (a map lookup keyed by string(b) does
// not allocate in Go).
var cmdNames = func() map[string]string {
	m := map[string]string{}
	for _, c := range []string{"GET", "SET", "MGET", "MSET", "DEL", "EXISTS", "UNLINK", "TOUCH", "INCR", "INCRBY", "DECR", "DECRBY",
		"ZINCRBY", "ZADD", "ZREVRANGE", "ZRANGE", "ZSCORE", "ZCARD", "HGET", "HSET", "HINCRBY", "LRANGE", "LPUSH", "RPUSH", "EXPIRE",
		"PING", "COMMAND", "SELECT", "READONLY", "READWRITE", "CLIENT", "AUTH", "QUIT", "MULTI", "EXEC", "WATCH", "SUBSCRIBE",
		"PSUBSCRIBE", "BLPOP", "BRPOP"} {
		m[c] = c
		m[strings.ToLower(c)] = c
	}
	return m
}()

func upper(b []byte) string {
	if c, ok := cmdNames[string(b)]; ok {
		return c
	}
	return strings.ToUpper(string(b))
}

func (p *Proxy) hot(key []byte) bool {
	now := time.Now()
	if p.cfg.Sample > 1 && p.sampleN.Add(1)%uint64(p.cfg.Sample) != 0 {
		return p.sk.Lower(key)*float64(p.cfg.Sample) >= p.cfg.HotThreshold
	}
	return p.sk.Add(key, now)*float64(p.cfg.Sample) >= p.cfg.HotThreshold
}

func (p *Proxy) handle(args [][]byte, st *connState) reply {
	p.S.Requests.Add(1)
	if st.tenant == nil {
		st.tenant = p.tenant("")
	}
	st.tenant.Requests.Add(1)
	cmd := upper(args[0])
	switch cmd {
	case "AUTH":
		// AUTH <tenant> <password> | AUTH <tenant>: the tenant for RU accounting and fair queueing
		if len(args) >= 2 {
			st.tenant = p.tenant(tenantName(string(args[1])))
		}
		return reply{raw: resp.AppendSimple(nil, "OK")}
	case "PING":
		if len(args) > 1 {
			return reply{raw: resp.AppendBulk(nil, args[1])}
		}
		return reply{raw: resp.AppendSimple(nil, "PONG")}
	case "COMMAND":
		return reply{raw: []byte("*0\r\n")}
	case "SELECT", "READONLY", "READWRITE":
		return reply{raw: resp.AppendSimple(nil, "OK")}
	case "CLIENT":
		// CLIENT CACHING no: this connection bypasses the cache (read-your-writes)
		if len(args) >= 3 && strings.EqualFold(string(args[1]), "CACHING") {
			st.noCache = strings.EqualFold(string(args[2]), "no")
		}
		return reply{raw: resp.AppendSimple(nil, "OK")}
	case "MULTI", "EXEC", "WATCH", "SUBSCRIBE", "PSUBSCRIBE", "BLPOP", "BRPOP":
		return reply{raw: resp.AppendError(nil, "ERR command not supported through the proxy")}
	}
	if len(args) < 2 {
		return reply{raw: resp.AppendError(nil, "ERR wrong number of arguments")}
	}
	key := args[1]
	shard := p.slots.Shard(key)
	switch cmd {
	case "MGET", "DEL", "EXISTS", "UNLINK", "TOUCH":
		for _, k := range args[2:] {
			if p.slots.Shard(k) != shard || cluster.Slot(k) != cluster.Slot(key) {
				return reply{raw: resp.AppendError(nil, "CROSSSLOT Keys in request don't hash to the same slot")}
			}
		}
	case "MSET":
		for i := 3; i < len(args); i += 2 {
			if cluster.Slot(args[i]) != cluster.Slot(key) {
				return reply{raw: resp.AppendError(nil, "CROSSSLOT Keys in request don't hash to the same slot")}
			}
		}
	}
	enc := resp.AppendCommand(nil, args...)
	if !p.cfg.Hot {
		return p.forward(st, shard, enc, cmd)
	}
	isHot := p.hot(key)
	if cacheable[cmd] && !st.noCache {
		if isHot {
			r := p.cache.get(enc, key, shard)
			t := st.tenant
			if r.raw != nil {
				t.CacheHits.Add(1)
				t.charge(cmd, ruHit(), false)
				return r
			}
			fn, ch := r.fn, r.ch
			return reply{fn: func() []byte {
				var b []byte
				if fn != nil {
					b = fn()
				} else {
					b = reply{ch: ch}.wait()
				}
				t.charge(cmd, ruRead(len(b)), false)
				return b
			}}
		}
		if raw, ok := p.cache.peek(enc); ok { // still cached from when it was hot
			st.tenant.CacheHits.Add(1)
			st.tenant.charge(cmd, ruHit(), false)
			return reply{raw: raw}
		}
	}
	if isHot && p.cfg.CombineWindow > 0 {
		switch cmd {
		case "INCR":
			if len(args) == 2 {
				return p.comb.add(key, nil, 1, false, shard)
			}
		case "INCRBY":
			if len(args) == 3 {
				if n, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
					return p.comb.add(key, nil, float64(n), false, shard)
				}
			}
		case "ZINCRBY":
			if len(args) == 4 {
				if f, err := strconv.ParseFloat(string(args[2]), 64); err == nil {
					return p.comb.add(key, args[3], f, true, shard)
				}
			}
		}
	}
	return p.forward(st, shard, enc, cmd)
}

// forward sends a command to its shard, through the shard's fair queue when
// isolation is on, and charges the tenant's RU when the reply arrives.
func (p *Proxy) forward(st *connState, shard int, enc []byte, cmd string) reply {
	t := st.tenant
	write := writeCmds[cmd]
	var ch <-chan backend.Result
	if p.cfg.Isolation && p.cfg.QuotaShape && t.cfg.QuotaRU > 0 {
		cost := t.estimate(cmd, len(enc))
		wait, ok := t.reserve(cost, time.Now(), p.cfg.MaxQuotaWait)
		if !ok {
			t.Refused.Add(1)
			return reply{raw: resp.AppendError(nil, "ERR tenant over RU quota")}
		}
		if wait > 0 {
			t.Throttled.Add(1)
			time.Sleep(wait)
		}
		ch = p.fq[shard].enqueue(t, cost, enc, t.cfg.Weight)
	} else if p.cfg.Isolation {
		ch = p.fq[shard].submit(t, t.estimate(cmd, len(enc)), enc)
	} else {
		ch = p.servers[shard].Go(enc)
	}
	return reply{ch: ch, acct: t, cmd: cmd, write: write, reqLen: len(enc)}
}

// ---------------------------------------------------------------- cache

type entry struct {
	enc      []byte // encoded command, also the cache key
	shard    int
	mu       sync.Mutex
	raw      []byte
	fetched  time.Time
	filling  chan struct{} // closed when the first fill completes (the lease)
	used     atomic.Int64  // last access, unix nanos
	inflight atomic.Bool
	hotSince time.Time
	dataKey  string
}

type cache struct {
	p   *Proxy
	mu  sync.RWMutex
	m   map[string]*entry
	max int
}

func newCache(p *Proxy, max int) *cache { return &cache{p: p, m: map[string]*entry{}, max: max} }

func (c *cache) peek(enc []byte) ([]byte, bool) {
	c.mu.RLock()
	e := c.m[string(enc)]
	c.mu.RUnlock()
	if e == nil {
		return nil, false
	}
	e.mu.Lock()
	raw, age := e.raw, time.Since(e.fetched)
	e.mu.Unlock()
	if raw == nil || age > c.p.cfg.Staleness {
		return nil, false
	}
	e.used.Store(time.Now().UnixNano())
	c.p.S.CacheHits.Add(1)
	c.p.S.observeAge(age)
	return raw, true
}

func (c *cache) get(enc, key []byte, shard int) reply {
	now := time.Now()
	c.mu.RLock()
	e := c.m[string(enc)]
	c.mu.RUnlock()
	if e == nil {
		c.mu.Lock()
		if e = c.m[string(enc)]; e == nil {
			if len(c.m) >= c.max {
				c.evictLocked()
			}
			e = &entry{enc: enc, shard: shard, filling: make(chan struct{}), hotSince: now, dataKey: string(key)}
			c.m[string(enc)] = e
			c.mu.Unlock()
			// this request holds the lease: it fills the entry
			e.used.Store(now.UnixNano())
			c.p.S.Fills.Add(1)
			ch := c.p.servers[shard].Go(enc)
			return reply{fn: func() []byte {
				res := <-ch
				if res.Err != nil {
					c.mu.Lock()
					delete(c.m, string(enc))
					c.mu.Unlock()
					close(e.filling)
					return resp.AppendError(nil, "ERR proxy: "+res.Err.Error())
				}
				e.mu.Lock()
				e.raw, e.fetched = res.Reply.Raw, time.Now()
				e.mu.Unlock()
				close(e.filling)
				return res.Reply.Raw
			}}
		}
		c.mu.Unlock()
	}
	e.used.Store(now.UnixNano())
	select {
	case <-e.filling:
	default:
		// someone else holds the lease: wait for their fill
		c.p.S.LeaseWaits.Add(1)
		return reply{fn: func() []byte {
			<-e.filling
			e.mu.Lock()
			raw := e.raw
			e.mu.Unlock()
			if raw == nil {
				res := <-c.p.servers[shard].Go(enc)
				if res.Err != nil {
					return resp.AppendError(nil, "ERR proxy: "+res.Err.Error())
				}
				return res.Reply.Raw
			}
			return raw
		}}
	}
	e.mu.Lock()
	raw, age := e.raw, now.Sub(e.fetched)
	e.mu.Unlock()
	if raw != nil && age <= c.p.cfg.Staleness {
		c.p.S.CacheHits.Add(1)
		c.p.S.observeAge(age)
		return reply{raw: raw}
	}
	// too old (refresh failing or not yet run): never serve past the bound
	c.p.S.StaleBypass.Add(1)
	return reply{ch: c.p.servers[shard].Go(enc)}
}

func (c *cache) evictLocked() {
	var oldest *entry
	for _, e := range c.m {
		if oldest == nil || e.used.Load() < oldest.used.Load() {
			oldest = e
		}
	}
	if oldest != nil {
		delete(c.m, string(oldest.enc))
	}
}

// refreshLoop is the active update: every Staleness/2, re-fetch entries
// that are still in use, and drop ones nobody has read for a while.
func (c *cache) refreshLoop() {
	period := c.p.cfg.Staleness / 2
	if period < time.Millisecond {
		period = time.Millisecond
	}
	t := time.NewTicker(period)
	for now := range t.C {
		c.mu.RLock()
		var es []*entry
		for _, e := range c.m {
			es = append(es, e)
		}
		c.mu.RUnlock()
		for _, e := range es {
			if now.Sub(time.Unix(0, e.used.Load())) > 2*time.Second {
				c.mu.Lock()
				delete(c.m, string(e.enc))
				c.mu.Unlock()
				continue
			}
			select {
			case <-e.filling:
			default:
				continue
			}
			if !e.inflight.CompareAndSwap(false, true) {
				continue
			}
			c.p.S.Refreshes.Add(1)
			ch := c.p.servers[e.shard].Go(e.enc)
			go func(e *entry) {
				res := <-ch
				if res.Err == nil {
					e.mu.Lock()
					e.raw, e.fetched = res.Reply.Raw, time.Now()
					e.mu.Unlock()
				}
				e.inflight.Store(false)
			}(e)
		}
	}
}

// ---------------------------------------------------------------- combining

type op struct {
	delta float64
	out   chan []byte
}

type batch struct {
	ops []op
}

type combiner struct {
	p       *Proxy
	mu      sync.Mutex
	batches map[string]*batch
}

func fmtScore(f float64) string {
	if f == float64(int64(f)) && f < 1e15 && f > -1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', 17, 64)
}

// add queues one increment; the first op of a (key, member) opens a batch
// that is flushed after CombineWindow as a single INCRBY or ZINCRBY.
func (c *combiner) add(key, member []byte, delta float64, zset bool, shard int) reply {
	bk := string(key) + "\x00" + string(member)
	ch := make(chan []byte, 1)
	c.mu.Lock()
	b := c.batches[bk]
	if b == nil {
		b = &batch{}
		c.batches[bk] = b
		k, m := append([]byte(nil), key...), append([]byte(nil), member...)
		time.AfterFunc(c.p.cfg.CombineWindow, func() { c.flush(bk, k, m, zset, shard) })
	}
	b.ops = append(b.ops, op{delta, ch})
	c.mu.Unlock()
	return reply{fn: func() []byte { return <-ch }}
}

func (c *combiner) flush(bk string, key, member []byte, zset bool, shard int) {
	c.mu.Lock()
	b := c.batches[bk]
	delete(c.batches, bk)
	c.mu.Unlock()
	var sum float64
	for _, o := range b.ops {
		sum += o.delta
	}
	c.p.S.CombinedOps.Add(int64(len(b.ops)))
	c.p.S.CombinedBatches.Add(1)
	var enc []byte
	if zset {
		enc = resp.AppendCommand(nil, []byte("ZINCRBY"), key, []byte(fmtScore(sum)), member)
	} else {
		enc = resp.AppendCommand(nil, []byte("INCRBY"), key, []byte(strconv.FormatInt(int64(sum), 10)))
	}
	res := <-c.p.servers[shard].Go(enc)
	if res.Err != nil || res.Reply.Type == '-' {
		raw := res.Reply.Raw
		if res.Err != nil {
			raw = resp.AppendError(nil, "ERR proxy: "+res.Err.Error())
		}
		for _, o := range b.ops {
			o.out <- raw
		}
		return
	}
	// The batch is applied as if its ops ran back to back in arrival order:
	// op i sees the final value minus the increments queued after it.
	var final float64
	if zset {
		final, _ = strconv.ParseFloat(string(res.Reply.Str), 64)
	} else {
		final = float64(res.Reply.Int)
	}
	after := 0.0
	for i := len(b.ops) - 1; i >= 0; i-- {
		v := final - after
		if zset {
			b.ops[i].out <- resp.AppendBulk(nil, []byte(fmtScore(v)))
		} else {
			b.ops[i].out <- resp.AppendInt(nil, int64(v))
		}
		after += b.ops[i].delta
	}
}

// ---------------------------------------------------------------- stats

type StatsJSON struct {
	Requests        int64            `json:"requests"`
	CacheHits       int64            `json:"cache_hits"`
	LeaseWaits      int64            `json:"lease_waits"`
	Fills           int64            `json:"fills"`
	Refreshes       int64            `json:"refreshes"`
	StaleBypass     int64            `json:"stale_bypass"`
	CombinedOps     int64            `json:"combined_ops"`
	CombinedBatches int64            `json:"combined_batches"`
	ShardCalls      []int64          `json:"shard_calls"`
	StaleHistMs     map[string]int64 `json:"served_age_histogram"`
	Cached          []CachedKey      `json:"cached"`
	Tenants         []TenantStats    `json:"tenants,omitempty"`
	FairQueued      int64            `json:"fair_queued"` // requests that waited in a shard's fair queue
}

type CachedKey struct {
	Key      string `json:"key"`
	HotSince int64  `json:"hot_since_unix_nano"`
}

func (p *Proxy) Snapshot() StatsJSON {
	s := StatsJSON{Requests: p.S.Requests.Load(), CacheHits: p.S.CacheHits.Load(), LeaseWaits: p.S.LeaseWaits.Load(),
		Fills: p.S.Fills.Load(), Refreshes: p.S.Refreshes.Load(), StaleBypass: p.S.StaleBypass.Load(),
		CombinedOps: p.S.CombinedOps.Load(), CombinedBatches: p.S.CombinedBatches.Load(), StaleHistMs: map[string]int64{}}
	for _, sv := range p.servers {
		s.ShardCalls = append(s.ShardCalls, sv.Calls.Load())
	}
	for i := range p.S.staleHist {
		label := ">1000"
		if i < len(staleBuckets) {
			label = "<=" + strconv.Itoa(int(staleBuckets[i]/time.Millisecond))
		}
		s.StaleHistMs[label] = p.S.staleHist[i].Load()
	}
	p.tmu.Lock()
	for _, t := range p.tenants {
		s.Tenants = append(s.Tenants, TenantStats{Name: t.cfg.Name, QuotaRU: t.cfg.QuotaRU, Weight: t.cfg.Weight, Requests: t.Requests.Load(),
			RU: float64(t.RUMilli.Load()) / 1000, OverQuota: t.OverQuota.Load(), Throttled: t.Throttled.Load(), Refused: t.Refused.Load(), CacheHits: t.CacheHits.Load()})
	}
	p.tmu.Unlock()
	sort.Slice(s.Tenants, func(i, j int) bool { return s.Tenants[i].Name < s.Tenants[j].Name })
	for _, q := range p.fq {
		s.FairQueued += q.Queued.Load()
	}
	p.cache.mu.RLock()
	seen := map[string]bool{}
	for _, e := range p.cache.m {
		if !seen[e.dataKey] {
			seen[e.dataKey] = true
			s.Cached = append(s.Cached, CachedKey{e.dataKey, e.hotSince.UnixNano()})
		}
	}
	p.cache.mu.RUnlock()
	return s
}

func (p *Proxy) statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(p.Snapshot()); err != nil {
		log.Print(err)
	}
}
