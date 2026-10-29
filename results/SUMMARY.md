# HotKeyProxy results

3-shard Redis Cluster, proxies and load generator on one 2-vCPU VM. Open-loop load at a fixed rate; latency is from each request's scheduled time. Shard load comes from each shard's INFO commandstats.

## Shard load and latency vs skew (live-room mix, 10,000 rooms)

Median of 3 repetitions per cell; p99 range across repetitions in brackets. Lost increments are summed over repetitions.

| skew | path | hottest shard calls | max/mean shard load | p50 ms | p99 ms | read hit ratio | lost increments | Redis CPU s | proxy CPU s | proxy req per CPU-s |
|---|---|---|---|---|---|---|---|---|---|---|
| 0.99 | direct to cluster | 72,628 | 1.09 | 0.38 | 2.6 [2.5 to 2.9] |  | 0 | 5.1 | 0.0 |  |
| 1.2 | direct to cluster | 81,104 | 1.22 | 0.38 | 2.5 [2.2 to 2.8] |  | 0 | 4.9 | 0.0 |  |
| 1.5 | direct to cluster | 95,554 | 1.43 | 0.37 | 2.4 [2.3 to 3.2] |  | 0 | 5.1 | 0.0 |  |
| 2 | direct to cluster | 114,690 | 1.72 | 0.39 | 3.0 [2.6 to 4.4] |  | 0 | 4.9 | 0.0 |  |
| 0.99 | plain proxy | 72,896 | 1.09 | 0.34 | 4.1 [3.3 to 6.6] |  | 0 | 6.5 | 12.6 | 15,873 |
| 1.2 | plain proxy | 81,370 | 1.22 | 0.35 | 4.5 [4.2 to 4.9] |  | 0 | 6.6 | 12.5 | 16,000 |
| 1.5 | plain proxy | 96,089 | 1.44 | 0.36 | 5.2 [4.5 to 5.5] |  | 0 | 6.6 | 12.8 | 15,576 |
| 2 | plain proxy | 115,182 | 1.73 | 0.36 | 4.7 [3.7 to 5.1] |  | 0 | 6.1 | 12.8 | 15,613 |
| 0.99 | hot-key proxy (cache only) | 63,900 | 1.06 | 0.35 | 4.2 [4.0 to 4.3] | 19% | 0 | 5.9 | 12.5 | 16,064 |
| 1.2 | hot-key proxy (cache only) | 54,376 | 1.08 | 0.35 | 4.3 [3.6 to 5.2] | 42% | 0 | 5.2 | 12.1 | 16,598 |
| 1.5 | hot-key proxy (cache only) | 45,933 | 1.19 | 0.35 | 4.2 [3.5 to 5.0] | 68% | 0 | 4.1 | 11.1 | 17,953 |
| 2 | hot-key proxy (cache only) | 40,755 | 1.39 | 0.35 | 3.9 [3.6 to 5.7] | 86% | 0 | 3.5 | 10.7 | 18,639 |

Age of cached replies served (hot-key proxy, skew 1.2, staleness bound 50 ms, first repetition): <=1 ms: 4.7%, <=2 ms: 3.7%, <=5 ms: 12.0%, <=10 ms: 19.9%, <=20 ms: 39.9%, <=50 ms: 19.8%

## Limited fan-out: 4 proxies, 64-entry caches, 100,000 rooms, skew 1.2

Groups = n: proxies are split into n groups and a key is always sent to its own group (ABase's limited fan-out hashing). n = 1 spreads every key over all 4 proxies; n = 4 pins each key to one proxy.

| groups | proxies per key | read hit ratio | max/mean proxy load | max/mean shard load | p99 ms |
|---|---|---|---|---|---|
| 1 | 4 | 32% | 1.01 | 1.08 | 13.9 |
| 2 | 2 | 39% | 1.00 | 1.08 | 18.3 |
| 4 | 1 | 47% | 1.21 | 1.08 | 10.9 |

## Detection delay

A cold room swaps places with the most popular room 10 s into the run. Time until the proxy caches the new viral room's viewer-count key: 177 ms, 173 ms, 177 ms (sketch half-life 1 s, hot threshold 200 decayed accesses).

## Write combining (skew 2.0)

Hot INCR/INCRBY/ZINCRBY on the same key and member within the window are merged into one shard write; each client still gets its exact running total. Median of 3 repetitions.

| window | shard calls | hottest shard calls | ops combined | batches | ops per batch | p50 ms (all commands) | INCRBY p99 ms | ZINCRBY p99 ms | read p99 ms | lost increments | lost gift score |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 87,998 | 40,751 | 0 | 0 | 0.00 | 0.33 | 3.5 | 3.7 | 3.4 | 0 | 0 |
| 500us | 73,794 | 30,259 | 48,075 | 33,853 | 1.42 | 1.03 | 6.7 | 6.8 | 5.9 | 0 | 0 |
| 2ms | 67,289 | 29,978 | 48,080 | 27,353 | 1.76 | 2.68 | 17.1 | 18.0 | 15.5 | 0 | 0 |

## Tenant isolation (RU quotas + weighted fair queueing per shard)

Victim tenant: the live-room mix at 2,000 req/s (quota 4,000 RU/s). Noisy tenant: top-200 leaderboard reads (ZREVRANGE over 1,000-member sets, about 2 RU each) at 300 req/s with a 600 RU/s quota, bursting to 10x for 10 s (t = 10-20 s). Caching off so every request reaches Redis; at most 8 requests in flight per shard. Victim latency inside the burst window (8-18 s of its run) and outside it. Median of repetitions.

| isolation | victim p50 ms | victim p99 in burst | victim p99 outside | noisy achieved req/s | noisy p99 ms | noisy over-quota requests | noisy throttled / refused | noisy RU | fair-queued requests |
|---|---|---|---|---|---|---|---|---|---|
| off | 0.70 | 4.6 | 1.5 | 1,200 | 5.3 | 0 | 0 / 0 | 72,000 | 0 |
| WFQ, soft quota | 0.70 | 3.9 | 1.4 | 1,200 | 4.5 | 26,702 | 0 / 0 | 72,000 | 60 |
| WFQ, enforced quota | 0.62 | 1.8 | 1.4 | 303 | 89,015.8 | 0 | 32,668 / 0 | 72,000 | 2 |

## Redis client-side caching (CLIENT TRACKING) vs the hot-key proxy

Tracking: 4 simulated application servers, each with a local cache invalidated by Redis (RESP2 redirect mode, one invalidation connection per shard). Same live-room mix and rate as the skew sweep; direct-to-cluster numbers from the skew sweep for reference. Median of repetitions.

| skew | path | hottest shard calls | max/mean shard load | p50 ms | p99 ms | read hit ratio | invalidations | Redis CPU s |
|---|---|---|---|---|---|---|---|---|
| 1.2 | direct to cluster | 81,104 | 1.22 | 0.38 | 2.5 |  |  | 4.9 |
| 1.2 | CLIENT TRACKING, 4 app servers | 40,478 | 1.17 | 0.50 | 2.2 | 69% | 28,127 | 3.9 |
| 1.2 | hot-key proxy | 54,381 | 1.08 | 0.37 | 5.3 | 42% |  | 5.3 |
| 2 | direct to cluster | 114,690 | 1.72 | 0.39 | 3.0 |  |  | 4.9 |
| 2 | CLIENT TRACKING, 4 app servers | 43,703 | 1.42 | 0.51 | 2.3 | 77% | 31,426 | 3.6 |
| 2 | hot-key proxy | 40,755 | 1.39 | 0.33 | 3.0 | 86% |  | 3.5 |

## Throughput per proxy core

Proxy pinned to one CPU with GOMAXPROCS=1; Redis and the load generator pinned to the other. Open loop at the offered rate, skew 1.2, 15 s. The core is saturated where achieved falls below offered or p99 climbs.

| proxy | offered req/s | achieved req/s | p50 ms | p99 ms | proxy CPU s | requests per proxy CPU-second |
|---|---|---|---|---|---|---|
| plain | 20,000 | 19,970 | 1.01 | 41.0 | 12.2 | 24,610 |
| plain | 40,000 | 38,051 | 470.25 | 873.6 | 12.6 | 47,733 |
| plain | 60,000 | 39,017 | 3890.42 | 7974.9 | 18.3 | 49,127 |
| plain | 80,000 | 41,298 | 7253.65 | 13930.3 | 23.3 | 51,546 |
| hot-key | 20,000 | 19,988 | 0.53 | 20.6 | 11.7 | 25,554 |
| hot-key | 40,000 | 39,921 | 15.82 | 99.5 | 12.5 | 47,923 |
| hot-key | 60,000 | 44,141 | 2662.00 | 5289.1 | 16.7 | 53,828 |
| hot-key | 80,000 | 43,485 | 5851.01 | 12452.7 | 22.6 | 53,004 |

