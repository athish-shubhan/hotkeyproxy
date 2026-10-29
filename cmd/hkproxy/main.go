// Command hkproxy runs the hot-key-aware RESP proxy.
package main

import (
	"flag"
	"log"
	"strconv"
	"strings"
	"time"

	"hotkeyproxy/proxy"
)

func main() {
	c := proxy.DefaultConfig()
	flag.StringVar(&c.Listen, "listen", c.Listen, "client address")
	flag.StringVar(&c.StatsAddr, "stats", c.StatsAddr, "HTTP stats address (empty = off)")
	flag.StringVar(&c.Seed, "seed", c.Seed, "any Redis Cluster node")
	flag.IntVar(&c.ConnsPerShard, "conns", c.ConnsPerShard, "pipelined connections per shard")
	flag.BoolVar(&c.Hot, "hot", c.Hot, "hot-key handling (false = plain proxy)")
	flag.Float64Var(&c.HotThreshold, "threshold", c.HotThreshold, "decayed accesses for a key to be hot")
	flag.DurationVar(&c.HalfLife, "half-life", c.HalfLife, "sketch decay half-life")
	flag.DurationVar(&c.Staleness, "staleness", c.Staleness, "max age of a served cached reply")
	flag.IntVar(&c.CacheEntries, "cache", c.CacheEntries, "max cached replies")
	flag.DurationVar(&c.CombineWindow, "combine", c.CombineWindow, "write-combining window (0 = off)")
	flag.IntVar(&c.Sample, "sample", c.Sample, "record 1 of N accesses in the sketch")
	tenants := flag.String("tenants", "", "tenant:quotaRU:weight,... (AUTH <tenant> selects one)")
	flag.BoolVar(&c.Isolation, "isolation", false, "RU-weighted fair queueing per shard")
	flag.IntVar(&c.MaxInFlight, "max-inflight", c.MaxInFlight, "isolation: outstanding requests per shard")
	flag.BoolVar(&c.QuotaShape, "quota-shape", false, "isolation: enforce RU quotas (wait for tokens, refuse past -max-quota-wait) instead of only deprioritizing")
	flag.DurationVar(&c.MaxQuotaWait, "max-quota-wait", 200*time.Millisecond, "quota-shape: longest a request may wait for tokens")
	flag.Parse()
	for _, t := range strings.Split(*tenants, ",") {
		f := strings.Split(t, ":")
		if len(f) != 3 {
			continue
		}
		q, _ := strconv.ParseFloat(f[1], 64)
		w, _ := strconv.ParseFloat(f[2], 64)
		c.Tenants = append(c.Tenants, proxy.TenantConfig{Name: f[0], QuotaRU: q, Weight: w})
	}
	var p *proxy.Proxy
	var err error
	for i := 0; i < 50; i++ { // the cluster may still be forming
		if p, err = proxy.New(c); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("hkproxy on %s, %d shards, hot=%v", c.Listen, len(p.Shards()), c.Hot)
	log.Fatal(p.ListenAndServe())
}
