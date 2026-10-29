package proxy

import (
	"bufio"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hotkeyproxy/backend"
	"hotkeyproxy/resp"
)

// fakeRedis is a single-node "cluster" that owns every slot and implements
// the handful of commands the tests use. It counts commands so tests can see
// what reached the backend.
type fakeRedis struct {
	ln    net.Listener
	mu    sync.Mutex
	ints  map[string]int64
	zsets map[string]map[string]float64
	calls atomic.Int64
	incrs atomic.Int64
}

func startFake(t *testing.T) *fakeRedis {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeRedis{ln: ln, ints: map[string]int64{}, zsets: map[string]map[string]float64{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return f
}

func (f *fakeRedis) serve(c net.Conn) {
	defer c.Close()
	r := resp.NewReader(c)
	w := bufio.NewWriter(c)
	port, _ := strconv.Atoi(strings.Split(f.ln.Addr().String(), ":")[1])
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		f.calls.Add(1)
		cmd := strings.ToUpper(string(args[0]))
		f.mu.Lock()
		var out []byte
		switch cmd {
		case "CLUSTER":
			out = []byte("*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:" + strconv.Itoa(port) + "\r\n")
		case "GET":
			v, ok := f.ints[string(args[1])]
			if ok {
				out = resp.AppendBulk(nil, []byte(strconv.FormatInt(v, 10)))
			} else {
				out = resp.AppendBulk(nil, nil)
			}
		case "INCRBY", "INCR":
			f.incrs.Add(1)
			d := int64(1)
			if cmd == "INCRBY" {
				d, _ = strconv.ParseInt(string(args[2]), 10, 64)
			}
			f.ints[string(args[1])] += d
			out = resp.AppendInt(nil, f.ints[string(args[1])])
		case "ZINCRBY":
			d, _ := strconv.ParseFloat(string(args[2]), 64)
			z := f.zsets[string(args[1])]
			if z == nil {
				z = map[string]float64{}
				f.zsets[string(args[1])] = z
			}
			z[string(args[3])] += d
			out = resp.AppendBulk(nil, []byte(fmtScore(z[string(args[3])])))
		default:
			out = resp.AppendSimple(nil, "OK")
		}
		f.mu.Unlock()
		w.Write(out)
		if r.Buffered() == 0 {
			w.Flush()
		}
	}
}

func newProxy(t *testing.T, f *fakeRedis, mod func(*Config)) *Proxy {
	c := DefaultConfig()
	c.Seed = f.ln.Addr().String()
	c.HotThreshold = 5
	mod(&c)
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func do(p *Proxy, args ...string) []byte {
	var a [][]byte
	for _, x := range args {
		a = append(a, []byte(x))
	}
	return p.handle(a, &connState{}).wait()
}

// Concurrent INCRBYs on a hot key are combined, yet every caller gets a
// distinct value, the values are exactly the running totals of some order,
// and nothing is lost.
func TestCombiningIsExactlyOnceAndLinearizableForCounters(t *testing.T) {
	f := startFake(t)
	p := newProxy(t, f, func(c *Config) { c.CombineWindow = 5 * time.Millisecond })
	for i := 0; i < 10; i++ { // make the key hot
		do(p, "INCRBY", "likes", "1")
	}
	base := f.ints["likes"]
	const n = 200
	vals := make([]int64, n)
	var wg sync.WaitGroup
	before := f.incrs.Load()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			raw := do(p, "INCRBY", "likes", "2")
			v, _ := strconv.ParseInt(strings.TrimSpace(string(raw[1:])), 10, 64)
			vals[i] = v
		}(i)
	}
	wg.Wait()
	if got := f.ints["likes"]; got != base+2*n {
		t.Fatalf("final %d, want %d", got, base+2*n)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	for i, v := range vals {
		if v != base+2*int64(i+1) {
			t.Fatalf("reply %d = %d, want %d: replies are not the running totals", i, v, base+2*int64(i+1))
		}
	}
	if sent := f.incrs.Load() - before; sent >= n {
		t.Fatalf("no combining: %d INCRBYs reached Redis for %d requests", sent, n)
	}
}

func TestZincrbyCombiningSums(t *testing.T) {
	f := startFake(t)
	p := newProxy(t, f, func(c *Config) { c.CombineWindow = 5 * time.Millisecond })
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); do(p, "ZINCRBY", "lb", "1.5", "alice") }()
	}
	wg.Wait()
	if got := f.zsets["lb"]["alice"]; got != 150 {
		t.Fatalf("score %v, want 150", got)
	}
}

// A hot key is served from the cache, and never from an entry older than
// the staleness bound.
func TestHotReadsCachedWithinStalenessBound(t *testing.T) {
	f := startFake(t)
	p := newProxy(t, f, func(c *Config) { c.Staleness = 20 * time.Millisecond; c.CombineWindow = 0 })
	f.ints["viewers"] = 7
	for i := 0; i < 10; i++ {
		do(p, "GET", "viewers")
	}
	calls := f.calls.Load()
	for i := 0; i < 1000; i++ {
		if got := string(do(p, "GET", "viewers")); got != "$1\r\n7\r\n" {
			t.Fatalf("got %q", got)
		}
	}
	if extra := f.calls.Load() - calls; extra > 100 {
		t.Fatalf("%d backend calls for 1000 hot reads: not cached", extra)
	}
	// a write elsewhere becomes visible within the bound (plus one refresh period)
	f.mu.Lock()
	f.ints["viewers"] = 8
	f.mu.Unlock()
	deadline := time.Now().Add(20*time.Millisecond + 15*time.Millisecond)
	for string(do(p, "GET", "viewers")) != "$1\r\n8\r\n" {
		if time.Now().After(deadline) {
			t.Fatal("stale value served past the staleness bound")
		}
		time.Sleep(time.Millisecond)
	}
	snap := p.Snapshot()
	if snap.StaleHistMs[">1000"] != 0 || snap.StaleHistMs["<=1000"] != 0 || snap.StaleHistMs["<=200"] != 0 || snap.StaleHistMs["<=100"] != 0 || snap.StaleHistMs["<=50"] != 0 {
		t.Fatalf("served replies older than the 20 ms bound: %v", snap.StaleHistMs)
	}
}

// Concurrent first reads of a newly hot key make one backend request (lease).
func TestLeaseCoalescesFill(t *testing.T) {
	f := startFake(t)
	p := newProxy(t, f, func(c *Config) { c.CombineWindow = 0; c.HotThreshold = 1 })
	before := f.calls.Load()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); do(p, "GET", "fresh") }()
	}
	wg.Wait()
	if n := f.calls.Load() - before; n > 5 {
		t.Fatalf("%d backend calls for 50 concurrent first reads", n)
	}
}

func TestCrossSlotRejected(t *testing.T) {
	f := startFake(t)
	p := newProxy(t, f, func(c *Config) {})
	if got := string(do(p, "MGET", "a", "b")); !strings.HasPrefix(got, "-CROSSSLOT") {
		t.Fatalf("got %q", got)
	}
	if got := string(do(p, "MGET", "{r}a", "{r}b")); strings.HasPrefix(got, "-") {
		t.Fatalf("same-slot MGET rejected: %q", got)
	}
}

// With one request in flight per shard, a victim's request submitted behind
// a noisy tenant's queue of heavy requests is dispatched almost at once.
func TestFairQueueServesVictimFirst(t *testing.T) {
	f := startFake(t)
	p := newProxy(t, f, func(c *Config) {
		c.Hot = false
		c.Isolation = true
		c.MaxInFlight = 1
	})
	noisy, victim := p.tenant("noisy"), p.tenant("victim")
	q := p.fq[0]
	enc := resp.AppendCommand(nil, []byte("GET"), []byte("k"))
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	wait := func(name string, ch <-chan backend.Result) {
		defer wg.Done()
		<-ch
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go wait("noisy", q.submit(noisy, 8, enc)) // heavy: 8 RU each
	}
	wg.Add(1)
	go wait("victim", q.submit(victim, 1, enc))
	wg.Wait()
	for i, n := range order {
		if n == "victim" {
			if i > 10 {
				t.Fatalf("victim completed %dth of %d", i+1, len(order))
			}
			return
		}
	}
	t.Fatal("victim never completed")
}

func TestRUCharging(t *testing.T) {
	if ruRead(100) != 1 || ruRead(4097) != 2 || ruWrite(2048) != 2 || ruWrite(10) != 1 {
		t.Fatal("RU rounding")
	}
}

func TestTenantReserve(t *testing.T) {
	tn := newTenant(TenantConfig{Name: "n", QuotaRU: 100, Weight: 1}, 1)
	now := tn.last
	// a full bucket (100 RU) admits 100 RU at once
	for i := 0; i < 100; i++ {
		if w, ok := tn.reserve(1, now, 200*time.Millisecond); !ok || w != 0 {
			t.Fatalf("request %d: wait %v ok %v", i, w, ok)
		}
	}
	// then each RU costs 10 ms of waiting, up to the 200 ms cap
	if w, ok := tn.reserve(10, now, 200*time.Millisecond); !ok || w != 100*time.Millisecond {
		t.Fatalf("got wait %v ok %v, want 100ms", w, ok)
	}
	if _, ok := tn.reserve(15, now, 200*time.Millisecond); ok {
		t.Fatal("a request that would wait 250 ms must be refused")
	}
	// the refused request took nothing: 10 RU (100 ms more debt) still fits
	if w, ok := tn.reserve(10, now, 200*time.Millisecond); !ok || w != 200*time.Millisecond {
		t.Fatalf("got wait %v ok %v, want 200ms", w, ok)
	}
	// after 1 s the bucket has earned 100 RU back: debt 20 RU, so 80 RU are free
	if w, ok := tn.reserve(80, now.Add(time.Second), 200*time.Millisecond); !ok || w != 0 {
		t.Fatalf("after refill: wait %v ok %v", w, ok)
	}
}
