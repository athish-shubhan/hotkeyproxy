// Command hkproxy runs the hot-key-aware RESP proxy.
package main

import (
	"flag"
	"fmt"
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
	ts, err := parseTenants(*tenants)
	if err != nil {
		log.Fatal(err)
	}
	c.Tenants = ts
	if err := c.Validate(); err != nil {
		log.Fatal(err) // before the retry loop: a bad flag is not a forming cluster
	}
	var p *proxy.Proxy
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

// parseTenants reads "name:quotaRU:weight,...". Malformed entries used to be
// skipped and bad numbers read as 0, so a typo silently meant "no quota".
func parseTenants(spec string) ([]proxy.TenantConfig, error) {
	var out []proxy.TenantConfig
	seen := map[string]bool{}
	for _, t := range strings.Split(spec, ",") {
		if t = strings.TrimSpace(t); t == "" {
			continue
		}
		f := strings.Split(t, ":")
		if len(f) != 3 || strings.TrimSpace(f[0]) == "" {
			return nil, fmt.Errorf("-tenants: bad entry %q (want name:quotaRU:weight)", t)
		}
		q, err1 := strconv.ParseFloat(f[1], 64)
		w, err2 := strconv.ParseFloat(f[2], 64)
		if err1 != nil || err2 != nil || q < 0 || w <= 0 {
			return nil, fmt.Errorf("-tenants: %q: quota must be a number >= 0 and weight a number > 0", t)
		}
		name := strings.TrimSpace(f[0])
		if seen[name] {
			return nil, fmt.Errorf("-tenants: %q given twice", name)
		}
		seen[name] = true
		out = append(out, proxy.TenantConfig{Name: name, QuotaRU: q, Weight: w})
	}
	return out, nil
}
