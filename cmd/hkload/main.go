// Command hkload is an open-loop load generator for a live-room workload on
// Redis Cluster, either directly (client-side slot routing) or through one or
// more proxies with limited fan-out hashing. Requests are scheduled at fixed
// times and latency is measured from the scheduled time, so a slow server
// shows up as latency instead of as a lower offered rate (no coordinated
// omission).
//
// Workload: rooms with Zipf popularity. Per request: 50% GET viewers count,
// 15% leaderboard read (ZREVRANGE top 10), 20% like (INCRBY), 10% gift
// (ZINCRBY on the leaderboard), 5% GET likes. Optionally the viral room
// changes mid-run, to measure how fast the proxy notices.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"hotkeyproxy/backend"
	"hotkeyproxy/cluster"
	"hotkeyproxy/proxy"
	"hotkeyproxy/resp"
)

type zipf struct{ cdf []float64 }

func newZipf(n int, s float64) *zipf {
	z := &zipf{cdf: make([]float64, n)}
	var t float64
	for i := range z.cdf {
		t += 1 / math.Pow(float64(i+1), s)
		z.cdf[i] = t
	}
	for i := range z.cdf {
		z.cdf[i] /= t
	}
	return z
}

func (z *zipf) sample(r *rand.Rand) int { return sort.SearchFloat64s(z.cdf, r.Float64()) }

type result struct {
	Mode              string             `json:"mode"`
	Proxies           int                `json:"proxies"`
	Groups            int                `json:"groups"`
	Skew              float64            `json:"skew"`
	Rate              float64            `json:"rate"`
	Sent              int64              `json:"sent"`
	Errors            int64              `json:"errors"`
	Achieved          float64            `json:"achieved_rps"`
	LatencyMs         map[string]float64 `json:"latency_ms"`
	ShardCalls        []int64            `json:"shard_calls"` // from INFO commandstats deltas
	MaxMeanShard      float64            `json:"max_mean_shard_load"`
	ProxyCPUSec       float64            `json:"proxy_cpu_sec"`
	RedisCPUSec       float64            `json:"redis_cpu_sec"`
	LostIncrements    int64              `json:"lost_increments"` // likes sent minus likes counted, over checked rooms
	LostGiftScore     float64            `json:"lost_gift_score"`
	ShiftAt           float64            `json:"shift_at_sec,omitempty"`
	Label             string             `json:"label,omitempty"`
	ProxyRequests     []int64            `json:"proxy_requests,omitempty"` // per proxy: shows how spread a hot key is
	CacheHits         int64              `json:"cache_hits"`
	HitRatioReads     float64            `json:"hit_ratio_reads"`
	CombinedOps       int64              `json:"combined_ops"`
	CombinedBatches   int64              `json:"combined_batches"`
	LeaseWaits        int64              `json:"lease_waits"`
	ServedAgeHist     map[string]int64   `json:"served_age_ms_hist,omitempty"`
	DetectMs          float64            `json:"viral_detect_ms,omitempty"` // shift -> new viral room cached
	ReqPerProxyCPUSec float64            `json:"req_per_proxy_cpu_sec,omitempty"`

	Tenant                string  `json:"tenant,omitempty"`
	Mix                   string  `json:"mix,omitempty"`
	TrackingHits          int64   `json:"tracking_hits,omitempty"`
	TrackingMisses        int64   `json:"tracking_misses,omitempty"`
	TrackingInvalidations int64   `json:"tracking_invalidations,omitempty"`
	TenantRU              float64 `json:"tenant_ru,omitempty"`
}

func fetchStats(urls string) []proxy.StatsJSON {
	var out []proxy.StatsJSON
	for _, u := range strings.Split(urls, ",") {
		if u == "" {
			continue
		}
		var s proxy.StatsJSON
		if r, err := http.Get(u); err == nil {
			json.NewDecoder(r.Body).Decode(&s)
			r.Body.Close()
		}
		out = append(out, s)
	}
	return out
}

func main() {
	mode := flag.String("mode", "proxy", "direct | proxy")
	seed := flag.String("seed", "127.0.0.1:7001", "cluster node")
	proxies := flag.String("proxies", "127.0.0.1:7100", "comma list of proxy addresses")
	groups := flag.Int("groups", 1, "limited fan-out: proxies are split into this many groups; a key always goes to its group")
	rate := flag.Float64("rate", 20000, "requests per second (open loop)")
	dur := flag.Duration("duration", 20*time.Second, "run length")
	rooms := flag.Int("rooms", 10000, "rooms")
	skew := flag.Float64("skew", 1.2, "Zipf s over rooms")
	shift := flag.Duration("shift-at", 0, "at this time a cold room (rooms/2) swaps places with the viral room (0 = never)")
	workers := flag.Int("workers", 256, "in-flight request limit")
	conns := flag.Int("conns", 4, "pipelined connections per target")
	pids := flag.String("proxy-pids", "", "comma list of proxy PIDs to measure CPU")
	rpids := flag.String("redis-pids", "", "comma list of Redis PIDs to measure CPU")
	out := flag.String("out", "", "append JSON result here")
	flush := flag.Bool("flush", true, "FLUSHALL every shard before the run")
	statsURLs := flag.String("proxy-stats", "", "comma list of proxy stats URLs (http://host:port)")
	label := flag.String("label", "", "free-form label stored with the result")
	tenant := flag.String("tenant", "", "proxy mode: AUTH <tenant> on every connection (RU accounting, fair queueing)")
	mix := flag.String("mix", "liveroom", "liveroom | heavy (leaderboard top-200 reads, uniform over -heavy-rooms rooms)")
	heavyRooms := flag.Int("heavy-rooms", 100, "heavy mix: rooms read")
	preload := flag.Int("preload-lb", 0, "before the run, give rooms 0..heavy-rooms-1 leaderboards of this many members")
	burstAt := flag.Duration("burst-at", 0, "start of a burst (0 = none)")
	burstDur := flag.Duration("burst-dur", 10*time.Second, "burst length")
	burstX := flag.Float64("burst-x", 10, "rate multiplier during the burst")
	window := flag.String("window", "", "from:to (e.g. 10s:20s): also report latency for requests due in this window and outside it")
	trackClients := flag.Int("tracking-clients", 4, "tracking mode: application servers, each with its own cache")
	flag.Parse()

	sd := backend.NewServer(*seed, 1)
	rep, err := sd.Do(resp.AppendCommand(nil, []byte("CLUSTER"), []byte("SLOTS")))
	if err != nil {
		log.Fatal(err)
	}
	m, err := cluster.FromClusterSlots(rep)
	if err != nil {
		log.Fatal(err)
	}
	shards := make([]*backend.Server, len(m.Addrs))
	for i, a := range m.Addrs {
		shards[i] = backend.NewServer(a, *conns)
	}
	var prox []*backend.Server
	var initCmd []byte
	if *tenant != "" {
		initCmd = resp.AppendCommand(nil, []byte("AUTH"), []byte(*tenant), []byte("x"))
	}
	for _, a := range strings.Split(*proxies, ",") {
		prox = append(prox, backend.NewServerInit(a, *conns, initCmd))
	}
	var tcs []*trackingClient
	if *mode == "tracking" {
		for i := 0; i < *trackClients; i++ {
			tc, err := newTrackingClient(m, *conns)
			if err != nil {
				log.Fatal(err)
			}
			tcs = append(tcs, tc)
		}
	}
	if *mode == "proxy" && len(prox)%*groups != 0 {
		log.Fatalf("%d proxies cannot be split into %d groups", len(prox), *groups)
	}
	per := len(prox) / *groups
	target := func(key []byte, r *rand.Rand) *backend.Server {
		if *mode == "direct" {
			return shards[m.Shard(key)]
		}
		h := fnv.New32a()
		h.Write(key)
		g := int(h.Sum32()) % *groups
		return prox[g*per+r.IntN(per)]
	}

	if *flush {
		for _, sh := range shards {
			sh.Do(resp.AppendCommand(nil, []byte("FLUSHALL")))
		}
	}
	if *preload > 0 {
		for room := 0; room < *heavyRooms; room++ {
			k := []byte("room:" + strconv.Itoa(room) + ":lb")
			args := [][]byte{[]byte("ZADD"), k}
			for u := 0; u < *preload; u++ {
				args = append(args, []byte(strconv.Itoa(u*7%1000)), []byte("user:"+strconv.Itoa(u)))
			}
			shards[m.Shard(k)].Do(resp.AppendCommand(nil, args...))
		}
	}
	// open-loop schedule, optionally with a burst at burstX times the rate
	n1 := int64(*rate * dur.Seconds())
	var n2, n3 int64
	if *burstAt > 0 {
		n1 = int64(*rate * burstAt.Seconds())
		n2 = int64(*rate * *burstX * burstDur.Seconds())
		n3 = int64(*rate * (dur.Seconds() - burstAt.Seconds() - burstDur.Seconds()))
	}
	dueAt := func(i int64) time.Duration {
		switch {
		case i < n1:
			return time.Duration(float64(i) / *rate * float64(time.Second))
		case i < n1+n2:
			return *burstAt + time.Duration(float64(i-n1)/(*rate**burstX)*float64(time.Second))
		}
		return *burstAt + *burstDur + time.Duration(float64(i-n1-n2) / *rate * float64(time.Second))
	}
	var winFrom, winTo time.Duration
	if *window != "" {
		a, b, _ := strings.Cut(*window, ":")
		winFrom, _ = time.ParseDuration(a)
		winTo, _ = time.ParseDuration(b)
	}
	before := commandstats(shards)
	statsBefore := fetchStats(*statsURLs)
	cpu0, rcpu0 := cpuOf(*pids), cpuOf(*rpids)
	z := newZipf(*rooms, *skew)
	total := n1 + n2 + n3
	start := time.Now()
	var winLat, outLat []time.Duration
	var next, errs atomic.Int64
	lat := make([][]time.Duration, 5)
	var mu sync.Mutex
	likes := make([]atomic.Int64, *rooms)
	gifts := make([]atomic.Int64, *rooms) // gift amounts summed (integers)
	var wg sync.WaitGroup
	names := []string{"get_viewers", "lb_read", "like_incr", "gift_zincr", "get_likes"}
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 77))
			local := make([][]time.Duration, 5)
			var lwin, lout []time.Duration
			for {
				i := next.Add(1) - 1
				if i >= total {
					break
				}
				off := dueAt(i)
				due := start.Add(off)
				if d := time.Until(due); d > 0 {
					time.Sleep(d)
				}
				room := z.sample(r)
				if *shift > 0 && time.Since(start) >= *shift {
					// a cold room goes viral: swap it with rank 0
					if room == 0 {
						room = *rooms / 2
					} else if room == *rooms/2 {
						room = 0
					}
				}
				rs := strconv.Itoa(room)
				var args [][]byte
				var kind int
				var key []byte
				u := r.Float64()
				if *mix == "heavy" {
					u = 2 // below
				}
				switch {
				case u == 2:
					rs = strconv.Itoa(r.IntN(*heavyRooms))
					kind, key = 1, []byte("room:"+rs+":lb")
					args = [][]byte{[]byte("ZREVRANGE"), key, []byte("0"), []byte("199"), []byte("WITHSCORES")}
				case u < 0.50:
					kind, key = 0, []byte("room:"+rs+":viewers")
					args = [][]byte{[]byte("GET"), key}
				case u < 0.65:
					kind, key = 1, []byte("room:"+rs+":lb")
					args = [][]byte{[]byte("ZREVRANGE"), key, []byte("0"), []byte("9"), []byte("WITHSCORES")}
				case u < 0.85:
					kind, key = 2, []byte("room:"+rs+":likes")
					args = [][]byte{[]byte("INCRBY"), key, []byte("1")}
					likes[room].Add(1)
				case u < 0.95:
					kind, key = 3, []byte("room:"+rs+":lb")
					amt := 1 + r.IntN(10)
					args = [][]byte{[]byte("ZINCRBY"), key, []byte(strconv.Itoa(amt)), []byte("user:" + strconv.Itoa(r.IntN(500)))}
					gifts[room].Add(int64(amt))
				default:
					kind, key = 4, []byte("room:"+rs+":likes")
					args = [][]byte{[]byte("GET"), key}
				}
				var res backend.Result
				if *mode == "tracking" {
					res = tcs[w%len(tcs)].do(strings.ToUpper(string(args[0])), key, resp.AppendCommand(nil, args...))
				} else {
					res = <-target(key, r).Go(resp.AppendCommand(nil, args...))
				}
				if res.Err != nil || res.Reply.Type == '-' {
					errs.Add(1)
				}
				l := time.Since(due)
				local[kind] = append(local[kind], l)
				if *window != "" {
					if off >= winFrom && off < winTo {
						lwin = append(lwin, l)
					} else {
						lout = append(lout, l)
					}
				}
			}
			mu.Lock()
			for k := range local {
				lat[k] = append(lat[k], local[k]...)
			}
			winLat = append(winLat, lwin...)
			outLat = append(outLat, lout...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	el := time.Since(start)
	after := commandstats(shards)
	res := result{Mode: *mode, Proxies: len(prox), Groups: *groups, Skew: *skew, Rate: *rate, Sent: total, Errors: errs.Load(),
		Achieved: float64(total) / el.Seconds(), LatencyMs: map[string]float64{}, ProxyCPUSec: cpuOf(*pids) - cpu0, RedisCPUSec: cpuOf(*rpids) - rcpu0,
		ShiftAt: shift.Seconds()}
	if *mode == "direct" {
		res.Proxies, res.Groups = 0, 0
	}
	var all []time.Duration
	for k, l := range lat {
		all = append(all, l...)
		p := pct(l)
		res.LatencyMs[names[k]+"_p99"] = p[1]
	}
	p := pct(all)
	res.LatencyMs["p50"], res.LatencyMs["p99"], res.LatencyMs["p999"] = p[0], p[1], p[2]
	if *window != "" {
		pw, po := pct(winLat), pct(outLat)
		res.LatencyMs["window_p50"], res.LatencyMs["window_p99"] = pw[0], pw[1]
		res.LatencyMs["outside_p50"], res.LatencyMs["outside_p99"] = po[0], po[1]
	}
	res.Tenant, res.Mix = *tenant, *mix
	for _, tc := range tcs {
		res.TrackingHits += tc.Hits.Load()
		res.TrackingMisses += tc.Misses.Load()
		res.TrackingInvalidations += tc.Invals.Load()
	}
	if res.TrackingHits+res.TrackingMisses > 0 {
		res.HitRatioReads = float64(res.TrackingHits) / float64(res.TrackingHits+res.TrackingMisses)
	}
	var sum, mx float64
	for i := range after {
		d := after[i] - before[i]
		res.ShardCalls = append(res.ShardCalls, d)
		sum += float64(d)
		mx = math.Max(mx, float64(d))
	}
	res.MaxMeanShard = mx / (sum / float64(len(after)))
	// No lost writes: the hottest rooms' counters and leaderboards must add up.
	for room := 0; room < 5 && room < *rooms; room++ {
		rs := strconv.Itoa(room)
		v, err := shards[m.Shard([]byte("room:"+rs+":likes"))].Do(resp.AppendCommand(nil, []byte("GET"), []byte("room:"+rs+":likes")))
		if err == nil {
			got, _ := strconv.ParseInt(string(v.Str), 10, 64)
			res.LostIncrements += likes[room].Load() - got
		}
		k := []byte("room:" + rs + ":lb")
		v, err = shards[m.Shard(k)].Do(resp.AppendCommand(nil, []byte("ZRANGE"), k, []byte("0"), []byte("-1"), []byte("WITHSCORES")))
		if err == nil {
			var s float64
			for i := 1; i < len(v.Elems); i += 2 {
				f, _ := strconv.ParseFloat(string(v.Elems[i].Str), 64)
				s += f
			}
			res.LostGiftScore += float64(gifts[room].Load()) - s
		}
	}
	res.Label = *label
	statsAfter := fetchStats(*statsURLs)
	for i := range statsAfter {
		rq := statsAfter[i].Requests - statsBefore[i].Requests
		res.ProxyRequests = append(res.ProxyRequests, rq)
		res.CacheHits += statsAfter[i].CacheHits - statsBefore[i].CacheHits
		res.CombinedOps += statsAfter[i].CombinedOps - statsBefore[i].CombinedOps
		res.CombinedBatches += statsAfter[i].CombinedBatches - statsBefore[i].CombinedBatches
		res.LeaseWaits += statsAfter[i].LeaseWaits - statsBefore[i].LeaseWaits
		for k, v := range statsAfter[i].StaleHistMs {
			if res.ServedAgeHist == nil {
				res.ServedAgeHist = map[string]int64{}
			}
			res.ServedAgeHist[k] += v - statsBefore[i].StaleHistMs[k]
		}
		if *shift > 0 {
			viral := "room:" + strconv.Itoa(*rooms/2) + ":viewers"
			for _, c := range statsAfter[i].Cached {
				if c.Key == viral {
					d := time.Duration(c.HotSince - start.Add(*shift).UnixNano())
					if res.DetectMs == 0 || float64(d)/1e6 < res.DetectMs {
						res.DetectMs = float64(d) / 1e6
					}
				}
			}
		}
	}
	var preq int64
	for _, v := range res.ProxyRequests {
		preq += v
	}
	if preq > 0 {
		res.HitRatioReads = float64(res.CacheHits) / (0.70 * float64(preq)) // reads are 70% of the mix
		if res.ProxyCPUSec > 0 {
			res.ReqPerProxyCPUSec = float64(preq) / res.ProxyCPUSec
		}
	}
	b, _ := json.Marshal(res)
	fmt.Println(string(b))
	if *out != "" {
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err == nil {
			f.Write(append(b, '\n'))
			f.Close()
		}
	}
}

func pct(d []time.Duration) [3]float64 {
	if len(d) == 0 {
		return [3]float64{}
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	q := func(p float64) float64 { return float64(d[int(p*float64(len(d)-1))]) / 1e6 }
	return [3]float64{q(0.5), q(0.99), q(0.999)}
}

// commandstats returns, per shard, the total number of commands executed.
func commandstats(shards []*backend.Server) []int64 {
	out := make([]int64, len(shards))
	for i, s := range shards {
		rep, err := s.Do(resp.AppendCommand(nil, []byte("INFO"), []byte("commandstats")))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(rep.Str), "\r\n") {
			if !strings.HasPrefix(line, "cmdstat_") || strings.HasPrefix(line, "cmdstat_info") || strings.HasPrefix(line, "cmdstat_cluster") {
				continue
			}
			if j := strings.Index(line, "calls="); j >= 0 {
				v := line[j+6:]
				if k := strings.IndexByte(v, ','); k >= 0 {
					v = v[:k]
				}
				n, _ := strconv.ParseInt(v, 10, 64)
				out[i] += n
			}
		}
	}
	return out
}

// cpuOf sums user+system CPU seconds of the given PIDs from /proc.
func cpuOf(pids string) float64 {
	var t float64
	for _, p := range strings.Split(pids, ",") {
		if p == "" {
			continue
		}
		b, err := os.ReadFile("/proc/" + p + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		f := strings.Fields(s[strings.LastIndexByte(s, ')')+2:])
		u, _ := strconv.ParseFloat(f[11], 64)
		k, _ := strconv.ParseFloat(f[12], 64)
		t += (u + k) / 100 // clock ticks, 100 Hz on Linux
	}
	return t
}
