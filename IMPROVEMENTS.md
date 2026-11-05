# Improvements log

## Round 1 (2026-11-05): backend and slot-map tests, a real-Redis test, a flaky test fixed, input checks, CI

### What changed

1. **Tests for `backend/`**, the pipelined connection pool every proxied request goes through (0% coverage
   before): 2,000 concurrent calls over 3 connections each get their own reply (FIFO matching), the per-connection
   init command goes first and its reply is not handed to a caller, a server that drops connections every 5
   commands yields errors or correct replies (never a wrong reply or a hang) and the pool reconnects, a dial
   failure returns an error, and `Close`.
2. **`backend.Server.Close`** (new). Nothing could stop a server's connection loops, so `proxy.New` leaked the
   seed connection used for `CLUSTER SLOTS`, which then reconnected forever; `hkproxy` retries `New` up to 50
   times while a cluster forms. `New` now closes it. `NewServer(addr, 0)` (`hkproxy -conns 0`) used to divide by
   zero on the first request; below 1 now means 1.
3. **`cluster.FromClusterSlots`** (untested before): a negative slot start **panicked** (index -1), an end past
   16383 was silently clipped, a start after the end and a port of 0 were accepted, and `host + ":" + port` gave
   `::1:7001` for IPv6 nodes, which `net.Dial` cannot parse. All are now errors or `net.JoinHostPort`
   (`[::1]:7001`). Tests cover multi-shard maps, the empty-host fallback, IPv6 and bad replies. The existing
   hash-tag test compared an expression with itself (`x != x`, flagged by staticcheck), so it could never fail; it
   now checks the real rule.
4. **Integration test against real Redis** (`proxy/redis_test.go`). It starts `redis-server` 7 in cluster mode
   owning all slots (skips if not installed), builds the slot map from the real `CLUSTER SLOTS` reply, then
   through the proxy: SET/GET, 8 connections x 250 `INCR` on one hot key with **0 lost increments** (checked
   directly on Redis), cache hits on the hot key, the new value served after the staleness bound, CROSSSLOT and
   refused `MULTI`.
5. **Flaky test fixed.** `TestFairQueueServesVictimFirst` failed on main in 5 of 30 runs under `-race`: it read
   completion order from racing goroutines while the fake backend answered during the submit loop. The fake now
   holds commands until everything is queued, and the test checks the order the backend received them in: the
   victim is dispatched right after the request already in flight. 0 failures in 70 runs with `-race`.
6. **Input checks.** `proxy.Config.Validate` (called by `New` and by `hkproxy` before its retry loop) rejects
   `ConnsPerShard < 1`, `Sample < 1` (0 meant no key was ever hot), non-positive hot-key settings, `QuotaShape`
   without `Isolation`, and bad tenants. `hkproxy -tenants` used to skip malformed entries and read bad numbers as
   0, so `noisy:6OO:1` meant **no quota**; it now fails with a message. A taken `-stats` port was ignored
   silently (the benchmark then read no stats); the listener is now bound before serving and its error returned.
7. **`hkload` (the benchmark client)**: a failed open or write of the results file now stops the run (it dropped
   the record silently), a failed `FLUSHALL` or leaderboard preload stops the run (the leaderboard reads would
   otherwise run on empty keys), and an unreadable `/stats` response is logged instead of being recorded as zeros
   without comment.
8. **CI** (`.github/workflows/ci.yml`): gofmt, `go vet`, `go test -race` with Redis 7 installed so the
   integration test runs, golangci-lint v2.5.0 (`.golangci.yml`). **README**: a Quick start.

### Why it matters

The README's headline numbers (hot-shard load, 0 lost increments) rest on the backend pool and the slot map,
which had no tests; now both have tests, and one test runs against real Redis. A flaky test in CI is worse than
none, so it was fixed rather than retried.

### How it was verified

```
$ go test -count=1 -v ./... | grep -E '^--- ' | awk '{print $2}' | sort | uniq -c    # main
     16 PASS:
$ go test -count=1 -v ./... | grep -E '^--- ' | awk '{print $2}' | sort | uniq -c    # this branch (redis-server 7.0.15 on PATH)
     28 PASS:
$ go test -race -count=1 ./...          # all packages ok
$ golangci-lint run ./...               # main: 23 issues; branch: 0 issues
$ go vet ./... && gofmt -l .            # clean
```

Coverage (`go test -cover ./...`): `backend` 0% -> 95.1%, `cluster` 34.9% -> 95.8%, `proxy` 67.6% -> 73.3%,
`cmd/hkproxy` 0% -> 35.3%; others unchanged.

Flaky test, main vs branch:

```
$ for i in 1 2 3; do go test -race -count=10 -run TestFairQueue ./proxy | grep -c '^--- FAIL'; done   # main
1
3
1
$ for i in $(seq 6); do go test -race -count=10 ./proxy | grep -E '^--- FAIL'; done | wc -l           # branch
0
```

New cluster tests on main's `cluster/slots.go`:

```
slots_test.go:79: IPv6 address "::1:7001", want [::1]:7001
slots_test.go:98: start after end: want an error
slots_test.go:98: bad port: want an error
panic: runtime error: index out of range [-1]
```

The backend change adds a read lock per call, so I re-ran README experiment E1 at s = 2.0 for the hot-key proxy
(3 repetitions, `-rate 10000 -duration 20s`, 3-shard cluster from `bench/cluster.sh`, the same `hkproxy` and
`hkload` flags as `bench/run_all.sh`) on main and on this branch, on this VM:

| s = 2.0, hot-key proxy, median of 3 | committed `results/runs.jsonl` | main, re-run | this branch |
|---|---|---|---|
| calls on the hottest shard | 40,755 | 40,752 | 40,749 |
| max / mean shard load | 1.39 | 1.39 | 1.39 |
| read hit ratio | 0.86 | 0.86 | 0.86 |
| lost increments + lost gift score | 0 | 0 | 0 |
| p50 / p99 latency | 0.35 / 3.87 ms | 0.25 / 1.58 ms | 0.25 / 2.08 ms |
| proxy requests per CPU-second | 18,639 | 13,029 | 12,953 |

The shard-load numbers reproduce. p99 and requests per CPU-second differ between main and the branch by about
as much as between repetitions (README: "absolute latency and throughput are box-limited and noisy").

### Claims I could not verify

- **Proxy requests per CPU-second** (README: plain 15,613, hot-key 18,639): main's own code gives 13,029 for the
  hot-key proxy on this VM, so the difference is the machine, not this change. Left unchanged.
- Not re-run: the rest of E1 (other skews, direct and plain-proxy arms), E2-E7, the per-core throughput runs and
  the CLIENT TRACKING comparison (the full `bench/run_all.sh` takes about 30 minutes). No README number was changed.

### Files touched

`backend/backend.go`, `backend/backend_test.go` (new), `cluster/slots.go`, `cluster/slots_test.go`, `proxy/proxy.go`,
`proxy/proxy_test.go`, `proxy/redis_test.go` (new), `cmd/hkproxy/main.go`, `cmd/hkproxy/main_test.go` (new),
`cmd/hkload/main.go`, `cmd/hkload/tracking.go`, `.github/workflows/ci.yml` (new), `.golangci.yml` (new),
`README.md`, `IMPROVEMENTS.md` (new).
