# HotKeyProxy design

## 1. Problem

In Redis Cluster a key lives on one shard. A viral live room turns a handful
of keys (viewer count, like counter, gift leaderboard) into a large share of
all traffic, and that shard saturates while the others idle. Adding shards
does not help: the hot key is still on one of them.

ByteDance's ABase (SIGMOD '25) handles this in its proxy layer. HotKeyProxy
reproduces those proxy-side mechanisms in a RESP-compatible Go proxy and
measures them on a live-room workload.

## 2. Request path

```
client --RESP--> proxy: parse (allocation-free) -> route by CRC16 slot
                    |  read of a hot key   -> active-update cache (lease on first fill)
                    |  INCRBY/ZINCRBY hot  -> write combiner (one command per window)
                    |  everything else     -> pipelined connection to the owning shard
```

- **Protocol**: RESP2, pipelining (replies leave in request order: one
  goroutine reads and dispatches, one writes replies as their futures
  resolve), inline commands. Multi-key commands must be in one slot
  (`-CROSSSLOT` otherwise, like Redis Cluster). `MULTI/EXEC`, blocking and
  pub/sub commands are refused.
- **Backend**: a few pipelined connections per shard; a writer goroutine
  batches every queued command into one `write`, a reader goroutine matches
  replies to callers in FIFO order.

## 3. Hot-key detection

A Space-Saving sketch (k counters, 16 shards with their own locks) counts
accesses per key with exponential decay (half-life 1 s). Space-Saving
guarantees every key with true count above N/k is tracked, and `count - err`
is a lower bound on its true count; a key is hot when that lower bound
reaches the threshold (200 decayed accesses), so a cold key is never
reported hot. Decay lets a cooling key drop out and a newly viral one take
its place. Property tests check both guarantees against exact counts at
Zipf s = 1.01 to 2.0.

## 4. Hot reads: active-update cache with a staleness bound

Reads of a hot key (`GET`, `ZREVRANGE`, `ZRANGE`, `ZSCORE`, `HGET`, `ZCARD`,
`LRANGE`) are cached per exact command. A background loop refreshes every
cached entry each `staleness/2` while it is still being read, so a hot key
never takes a miss storm when an entry expires (ABase's active-update LRU).
An entry older than the staleness bound is never served; the request goes to
Redis instead (this only happens if refreshes fail).

**First fill uses a lease**: the first request for a newly hot key fetches
it; concurrent requests wait for that fill instead of all going to Redis
(Facebook's memcache leases, NSDI '13).

**Consistency**: bounded staleness. A read through the proxy may be up to
`staleness` (50 ms) old, including after a write through the same proxy. A
connection that needs read-your-writes sends `CLIENT CACHING no` and bypasses
the cache. Writes are never cached or delayed past being durable in Redis.

## 5. Hot writes: combining

`INCR`, `INCRBY` and `ZINCRBY` on a hot key are collected for a short window
and sent as one command with the summed increment. It is **off by default**
(`-combine 0`) and meant for write storms on a key whose shard is saturated. When Redis
replies with the new total, each caller gets the value its own increment
would have produced if the batch had run back to back in arrival order:
`final - (increments queued after it)`. A caller is answered only after the
combined command is applied, so combining loses nothing on a proxy crash
(the client simply gets no reply, as with any in-flight request). The cost is
up to one window of added latency, and it is paid by more than the combined
writes: RESP replies on a connection leave in request order, so every read
pipelined behind a combined write on the same connection waits for the
window too (head-of-line blocking). The first benchmark run had combining on
by default and showed exactly this: p50 for *all* commands rose from 0.35 ms
to 2.7 ms at skew 2.0. With a 500 us window at skew 2.0 the hottest shard
takes 26% fewer calls and p50 rises from 0.33 to 1.03 ms. A test checks that 200 concurrent
increments produce exactly the running totals and the right final value.

## 6. Limited fan-out (in the client)

With P proxies, full fan-out sends any key to any proxy: the hot key's load
spreads over all of them, but every proxy must cache every hot key. ABase
splits a tenant's proxies into n groups and always sends a key to its own
group: each proxy caches only 1/n of the hot set (higher hit ratio for a
fixed cache), and a hot key is spread over only P/n proxies. `hkload -groups`
implements this routing; the benchmark sweeps n.

## 7. Tenant isolation: request units and fair queueing

A simplified version of ABase's dual-layer scheme (`proxy/tenant.go`). A
connection's tenant is set with `AUTH <tenant>`.

- **Request units**: each request is charged after it completes: a read
  that reached Redis costs 1 RU per started 4 KiB of reply, a write 1 RU per
  started 1 KiB of request, a read served from the proxy cache 0.25 RU.
- **Layer 1, per tenant**: an RU/s quota as a token bucket (burst = one
  second of quota). Two modes:
  - *soft* (ABase's default behaviour as we read it): a request that finds
    the bucket empty is not rejected; it is scheduled at a tenth of its
    tenant's weight, so it only gets capacity the others leave unused;
  - *shape* (`-quota-shape`): the quota is enforced. `Tenant.reserve` lets
    the bucket go into debt and the request waits on its own connection
    until the debt is earned back; a request that would wait longer than
    `-max-quota-wait` (200 ms) is refused with an error and takes nothing.
- **Layer 2, per shard**: weighted fair queueing in front of each shard. At
  most `-max-inflight` requests per shard are outstanding (Redis executes a
  connection's commands in order, so anything already sent cannot be
  reordered); the rest wait ordered by virtual finish time, where each
  request's cost is a per-tenant, per-command moving average of observed RU,
  so heavy reads pay in queue position.

What E5 is designed to separate: fair queueing can only reorder work that
queues at a shard. If the victim's latency comes from something else on the
path (CPU shared with the noisy tenant's parsing, reply encoding and network
work), reordering cannot help and only refusing or delaying the noisy work
can.

Result (E5): soft quotas left the victim's burst p99 at 3.9 ms (4.6 ms with no
isolation) and fair queueing queued only 60 requests in a run, so shards were
not the bottleneck; enforced quotas brought it to 1.8 ms (1.4 ms outside the
burst). Known gap: the proxy reads a connection's requests one at a time, and
each waits for tokens before the next is read, so the token debt seen by any
one request stays under `-max-quota-wait` and nothing is refused; an open-loop
tenant's backlog then queues in its connection (p99 89 s). The fix is to refuse
by the age of the oldest unread request (or a per-tenant queue length), which
is what a production limiter would track.

## 8. Baseline: Redis client-side caching (CLIENT TRACKING)

`hkload -mode tracking` simulates N application servers, each with a local
cache of read replies kept coherent by Redis invalidations (RESP2 redirect
mode: per shard, one connection subscribes to `__redis__:invalidate` and
the data connections enable `CLIENT TRACKING ... REDIRECT <id>`). A read
stores its reply only if no invalidation for the key arrived between sending
the read and receiving the reply (the documented race for two-connection
mode). Staleness is bounded by invalidation delivery, not by a timer.

The trade against the proxy: tracking needs no extra hop and caches every
key a server reads (the simulated caches are unbounded), so at moderate skew (s = 1.2) it takes more load off the
hottest shard than a proxy that caches only detected hot keys; but each
application server holds its own copy, so a key that is hot everywhere is
fetched once per server and invalidated once per server, and at s = 2.0 the
proxy's single shared copy wins. Tracking also has to live in every client
library, while the proxy is one deployment.

## 9. Throughput per proxy core

The proxy is pinned to one vCPU with `GOMAXPROCS=1` and offered a fixed open-
loop rate; Redis and the load generator are pinned to the other vCPU. The core
is saturated where achieved throughput falls below offered or p99 climbs
past the queueing knee. Requests per proxy CPU-second is from `/proc`.

## 10. What is measured

- **max/mean shard load** from each shard's `INFO commandstats` (1.0 means
  perfectly even), so the metric does not depend on the proxy's own
  counters;
- latency p50/p99/p999 from each request's scheduled time (open loop: no
  coordinated omission), per operation type;
- read hit ratio, lease waits, combined ops and batches, the age
  distribution of served cached replies;
- **lost writes**: after the run, the hottest rooms' counters and leaderboard
  scores must equal what the load generator sent;
- CPU seconds of the proxy and of Redis (from `/proc`), and proxy requests per
  CPU-second;
- detection delay: a cold room swaps places with the most popular room
  mid-run; the time until the proxy caches the new viral room's key.

## 11. Validity threats

- Everything (3 Redis shards, proxies, load generator) shares a 2-vCPU VM, so
  latency and throughput are box-limited; shard-load ratios and hit ratios
  are not.
- Redis runs with `io-threads 1` (the default); a hot key is still
  single-threaded per shard in Redis 7.
- Tenant isolation is a simplified version of ABase's scheme (one proxy,
  RU estimates from moving averages, no cross-proxy quota coordination);
  anything below the proxy (ABase's storage layer) is not reproduced.
- The CLIENT TRACKING baseline simulates application servers inside the
  load generator, on the same box as everything else.

## 12. Interview defence notes

- *Why is a hot key a single-shard problem even with I/O threads?* Redis 6+
  I/O threads parallelise socket reads/writes and parsing, but command
  execution stays on the main thread, and a key lives on one shard.
- *Count-Min vs Space-Saving?* Count-Min estimates any key's count with
  one-sided error but cannot list the heavy hitters without a heap; Space-Saving
  keeps exactly the candidates with error bounds and is cheaper for top-k.
- *What breaks with write combining?* Latency (up to the window), and the
  per-caller values are those of one serial order of the batch, not the order
  Redis would have chosen; commands with conditions (e.g. `INCRBY` against a
  cap via Lua) cannot be combined.
- *What is coordinated omission?* A closed-loop client that waits for each
  reply before sending the next stops sending when the server stalls, so the
  stall is under-sampled. Scheduling requests at fixed times and measuring
  from the scheduled time counts the stall for every request that should have
  been sent.
- *Why did weighted fair queueing not protect the victim tenant?* Fair
  queueing reorders requests waiting for a shard. Here shards were rarely
  saturated (60 requests queued in a run); the noisy tenant hurt the victim
  through shared CPU and sockets on the proxy and the box. Only stopping the
  noisy work (an enforced quota) helped: victim burst p99 4.6 to 1.8 ms.
