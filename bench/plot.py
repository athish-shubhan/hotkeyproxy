#!/usr/bin/env python3
"""Figures and results/SUMMARY.md for HotKeyProxy."""
import json
import os
import statistics as st
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

src, out = sys.argv[1], sys.argv[2]
R = [json.loads(l) for l in open(src) if l.strip()]
md = ["# HotKeyProxy results", "",
      "3-shard Redis Cluster, proxies and load generator on one 2-vCPU VM. Open-loop load at a fixed rate; "
      "latency is from each request's scheduled time. Shard load comes from each shard's INFO commandstats.", ""]

modes = [("skew direct", "direct to cluster"), ("skew plain-proxy", "plain proxy"), ("skew hot-proxy", "hot-key proxy (cache only)")]
skews = sorted({r["skew"] for r in R if r.get("label", "").startswith("skew")})


def med(rs, f):
    v = [f(r) for r in rs]
    v = [x for x in v if x is not None]
    return st.median(v) if v else None


if skews:
    reps = max(len([x for x in R if x.get("label") == "skew direct" and x["skew"] == s]) for s in skews)
    md += ["## Shard load and latency vs skew (live-room mix, 10,000 rooms)", "",
           f"Median of {reps} repetitions per cell; p99 range across repetitions in brackets. Lost increments are summed over repetitions.", "",
           "| skew | path | hottest shard calls | max/mean shard load | p50 ms | p99 ms | read hit ratio | lost increments | Redis CPU s | proxy CPU s | proxy req per CPU-s |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    for lab, name in modes:
        xs, ys, ps = [], [], []
        for s in skews:
            rs = [x for x in R if x.get("label") == lab and x["skew"] == s]
            if not rs:
                continue
            load = med(rs, lambda r: r["max_mean_shard_load"])
            p99s = [r["latency_ms"]["p99"] for r in rs]
            xs.append(s)
            ys.append(load)
            ps.append(st.median(p99s))
            hit = med(rs, lambda r: r.get("hit_ratio_reads"))
            rpc = med(rs, lambda r: r.get("req_per_proxy_cpu_sec") or None)
            md.append(f"| {s} | {name} | {med(rs, lambda r: max(r['shard_calls'])):,.0f} | {load:.2f} | {med(rs, lambda r: r['latency_ms']['p50']):.2f} | "
                      f"{st.median(p99s):.1f} [{min(p99s):.1f} to {max(p99s):.1f}] | "
                      f"{'' if lab != 'skew hot-proxy' else f'{100*hit:.0f}%'} | {sum(r['lost_increments'] for r in rs)} | "
                      f"{med(rs, lambda r: r['redis_cpu_sec']):.1f} | {med(rs, lambda r: r['proxy_cpu_sec']):.1f} | "
                      f"{'' if not rpc else f'{rpc:,.0f}'} |")
        axes[0].plot(xs, ys, "-o", label=name)
        axes[1].plot(xs, ps, "-o", label=name)
    axes[0].set_title("max / mean shard load (1.0 = perfectly even)")
    axes[1].set_title("p99 latency (ms), median of repetitions")
    for ax in axes:
        ax.set_xlabel("Zipf s over rooms")
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "shard_load_vs_skew.png"), dpi=140)
    md.append("")
    hp = next((x for x in R if x.get("label") == "skew hot-proxy" and x["skew"] == 1.2), None)
    if hp and hp.get("served_age_ms_hist"):
        h = hp["served_age_ms_hist"]
        order = ["<=1", "<=2", "<=5", "<=10", "<=20", "<=50", "<=100", "<=200", "<=1000", ">1000"]
        tot = sum(h.get(k, 0) for k in order) or 1
        md += ["Age of cached replies served (hot-key proxy, skew 1.2, staleness bound 50 ms, first repetition): " +
               ", ".join(f"{k} ms: {100*h.get(k,0)/tot:.1f}%" for k in order if h.get(k, 0)), ""]

fo = [r for r in R if r.get("label") == "fanout"]
if fo:
    md += ["## Limited fan-out: 4 proxies, 64-entry caches, 100,000 rooms, skew 1.2", "",
           "Groups = n: proxies are split into n groups and a key is always sent to its own group (ABase's limited fan-out hashing). "
           "n = 1 spreads every key over all 4 proxies; n = 4 pins each key to one proxy.", "",
           "| groups | proxies per key | read hit ratio | max/mean proxy load | max/mean shard load | p99 ms |", "|---|---|---|---|---|---|"]
    for r in sorted(fo, key=lambda r: r["groups"]):
        pr = r["proxy_requests"]
        md.append(f"| {r['groups']} | {4 // r['groups']} | {100*r['hit_ratio_reads']:.0f}% | {max(pr)/st.mean(pr):.2f} | {r['max_mean_shard_load']:.2f} | {r['latency_ms']['p99']:.1f} |")
    md.append("")

de = [r for r in R if r.get("label") == "detect"]
if de:
    ms = [r["viral_detect_ms"] for r in de if r.get("viral_detect_ms")]
    md += ["## Detection delay", "",
           f"A cold room swaps places with the most popular room 10 s into the run. Time until the proxy caches the new viral room's "
           f"viewer-count key: {', '.join(f'{m:.0f} ms' for m in ms)} (sketch half-life 1 s, hot threshold 200 decayed accesses).", ""]

co = [r for r in R if r.get("label", "").startswith("combine")]
if co:
    md += ["## Write combining (skew 2.0)", "",
           "Hot INCR/INCRBY/ZINCRBY on the same key and member within the window are merged into one shard write; each client still gets "
           "its exact running total. Median of 3 repetitions.", "",
           "| window | shard calls | hottest shard calls | ops combined | batches | ops per batch | p50 ms (all commands) | INCRBY p99 ms | ZINCRBY p99 ms | read p99 ms | lost increments | lost gift score |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for w in ["0", "500us", "2ms"]:
        rs = [r for r in co if r["label"] == "combine " + w]
        if not rs:
            continue
        ops, bat = med(rs, lambda r: r["combined_ops"]), med(rs, lambda r: r["combined_batches"])
        md.append(f"| {w} | {med(rs, lambda r: sum(r['shard_calls'])):,.0f} | {med(rs, lambda r: max(r['shard_calls'])):,.0f} | {ops:,.0f} | {bat:,.0f} | {ops/bat if bat else 0:.2f} | "
                  f"{med(rs, lambda r: r['latency_ms']['p50']):.2f} | {med(rs, lambda r: r['latency_ms']['like_incr_p99']):.1f} | "
                  f"{med(rs, lambda r: r['latency_ms']['gift_zincr_p99']):.1f} | {med(rs, lambda r: r['latency_ms']['get_viewers_p99']):.1f} | "
                  f"{sum(r['lost_increments'] for r in rs)} | {sum(r['lost_gift_score'] for r in rs):.0f} |")
    md.append("")

# ---------------------------------------------------------------- E5 tenant isolation
iso = [r for r in R if r.get("label", "").startswith("isolation=")]
if iso:
    md += ["## Tenant isolation (RU quotas + weighted fair queueing per shard)", "",
           "Victim tenant: the live-room mix at 2,000 req/s (quota 4,000 RU/s). Noisy tenant: top-200 leaderboard reads (ZREVRANGE "
           "over 1,000-member sets, about 2 RU each) at 300 req/s with a 600 RU/s quota, bursting to 10x for 10 s (t = 10-20 s). "
           "Caching off so every request reaches Redis; at most 8 requests in flight per shard. Victim latency inside the burst "
           "window (8-18 s of its run) and outside it. Median of repetitions.", "",
           "| isolation | victim p50 ms | victim p99 in burst | victim p99 outside | noisy achieved req/s | noisy p99 ms | noisy over-quota requests | noisy throttled / refused | noisy RU | fair-queued requests |",
           "|---|---|---|---|---|---|---|---|---|---|"]
    names = {"false": "off", "true": "WFQ, soft quota", "off": "off", "soft": "WFQ, soft quota", "shape": "WFQ, enforced quota"}
    for on in ("false", "true", "off", "soft", "shape"):
        v = [r for r in iso if r["label"] == f"isolation={on} victim"]
        n = [r for r in iso if r["label"] == f"isolation={on} noisy"]
        t = [r for r in iso if r["label"] == f"isolation={on} tenants"]
        if not v:
            continue
        def tstat(r, name, k):
            for x in r.get("tenants") or []:
                if x["name"] == name:
                    return x.get(k)
            return None
        md.append(f"| {names[on]} | {med(v, lambda r: r['latency_ms']['window_p50'] if 'window_p50' in r['latency_ms'] else r['latency_ms']['p50']):.2f} | "
                  f"{med(v, lambda r: r['latency_ms'].get('window_p99')):.1f} | {med(v, lambda r: r['latency_ms'].get('outside_p99')):.1f} | "
                  f"{med(n, lambda r: r['achieved_rps']):,.0f} | {med(n, lambda r: r['latency_ms']['p99']):,.1f} | "
                  f"{(med(t, lambda r: tstat(r, 'noisy', 'over_quota_requests')) or 0):,.0f} | "
                  f"{(med(t, lambda r: tstat(r, 'noisy', 'throttled_requests')) or 0):,.0f} / {(med(t, lambda r: tstat(r, 'noisy', 'refused_requests')) or 0):,.0f} | "
                  f"{(med(t, lambda r: tstat(r, 'noisy', 'ru')) or 0):,.0f} | "
                  f"{(med(t, lambda r: r.get('fair_queued')) or 0):,.0f} |")
    md.append("")

# ---------------------------------------------------------------- E6 client-side caching
tr = [r for r in R if r.get("label") in ("tracking", "tracking-compare hot-proxy")]
if tr:
    md += ["## Redis client-side caching (CLIENT TRACKING) vs the hot-key proxy", "",
           "Tracking: 4 simulated application servers, each with a local cache invalidated by Redis (RESP2 redirect mode, "
           "one invalidation connection per shard). Same live-room mix and rate as the skew sweep; direct-to-cluster numbers "
           "from the skew sweep for reference. Median of repetitions.", "",
           "| skew | path | hottest shard calls | max/mean shard load | p50 ms | p99 ms | read hit ratio | invalidations | Redis CPU s |",
           "|---|---|---|---|---|---|---|---|---|"]
    for sk in sorted({r["skew"] for r in tr}):
        rows = [("direct to cluster", [r for r in R if r.get("label") == "skew direct" and r["skew"] == sk]),
                ("CLIENT TRACKING, 4 app servers", [r for r in tr if r["label"] == "tracking" and r["skew"] == sk]),
                ("hot-key proxy", [r for r in tr if r["label"] == "tracking-compare hot-proxy" and r["skew"] == sk])]
        for name, rs in rows:
            if not rs:
                continue
            if name.startswith("CLIENT"):
                hit = med(rs, lambda r: r.get("tracking_hits", 0) / max(1, r.get("tracking_hits", 0) + r.get("tracking_misses", 0)))
                inv = f"{med(rs, lambda r: r.get('tracking_invalidations', 0)):,.0f}"
            elif name.startswith("hot"):
                hit, inv = med(rs, lambda r: r.get("hit_ratio_reads")), ""
            else:
                hit, inv = None, ""
            md.append(f"| {sk} | {name} | {med(rs, lambda r: max(r['shard_calls'])):,.0f} | {med(rs, lambda r: r['max_mean_shard_load']):.2f} | "
                      f"{med(rs, lambda r: r['latency_ms']['p50']):.2f} | {med(rs, lambda r: r['latency_ms']['p99']):.1f} | "
                      f"{'' if hit is None else f'{100*hit:.0f}%'} | {inv} | {med(rs, lambda r: r['redis_cpu_sec']):.1f} |")
    md.append("")

# ---------------------------------------------------------------- E7 per-core throughput
pc = [r for r in R if r.get("label", "").startswith("percore")]
if pc:
    md += ["## Throughput per proxy core", "",
           "Proxy pinned to one CPU with GOMAXPROCS=1; Redis and the load generator pinned to the other. Open loop at the offered "
           "rate, skew 1.2, 15 s. The core is saturated where achieved falls below offered or p99 climbs.", "",
           "| proxy | offered req/s | achieved req/s | p50 ms | p99 ms | proxy CPU s | requests per proxy CPU-second |", "|---|---|---|---|---|---|---|"]
    for r in sorted(pc, key=lambda r: (r["label"], r["rate"])):
        md.append(f"| {'hot-key' if r['label'].endswith('true') else 'plain'} | {r['rate']:,.0f} | {r['achieved_rps']:,.0f} | {r['latency_ms']['p50']:.2f} | "
                  f"{r['latency_ms']['p99']:.1f} | {r['proxy_cpu_sec']:.1f} | {r.get('req_per_proxy_cpu_sec') or 0:,.0f} |")
    md.append("")

open(os.path.join(out, "SUMMARY.md"), "w").write("\n".join(md) + "\n")
print("\n".join(md))
