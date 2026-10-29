#!/usr/bin/env bash
# Reproduces results/: shard load and latency vs skew (direct, plain proxy,
# hot-key proxy), limited fan-out across 4 proxies, viral-room detection
# delay, and write combining on/off. Needs redis-server and redis-cli.
# About 20 minutes.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/hkproxy ./cmd/hkproxy
go build -o bin/hkload ./cmd/hkload
OUT=results/runs.jsonl
PHASE=${1:-all}   # all | base (E1-E4) | more (E5-E7) | e5
mkdir -p results
[[ $PHASE == all ]] && : > "$OUT"
RATE=${RATE:-10000}
DUR=${DUR:-20s}
bench/cluster.sh stop >/dev/null 2>&1 || true
bench/cluster.sh start
RPIDS=$(pgrep -f "redis-server .*:700[1-3]" | paste -sd, -)
PIDS=()
proxy() { # port statsport args...
  local port=$1 sport=$2; shift 2
  ./bin/hkproxy -listen 127.0.0.1:$port -stats 127.0.0.1:$sport "$@" > /tmp/hkproxy-$port.log 2>&1 &
  PIDS+=($!)
  until curl -sf 127.0.0.1:$sport >/dev/null; do sleep 0.1; done
}
stop_proxies() { for p in "${PIDS[@]}"; do kill $p 2>/dev/null || true; done; wait 2>/dev/null || true; PIDS=(); }
trap 'stop_proxies; bench/cluster.sh stop' EXIT

if [[ $PHASE == all || $PHASE == base ]]; then
echo "== E1: skew sweep (3 repetitions; hot-key proxy with caching only, combining off)"
for rep in 1 2 3; do
for s in 0.99 1.2 1.5 2.0; do
  ./bin/hkload -mode direct -rate $RATE -duration $DUR -skew $s -redis-pids $RPIDS -label "skew direct" -out $OUT > /dev/null
  proxy 7100 7200 -hot=false
  ./bin/hkload -mode proxy -proxies 127.0.0.1:7100 -proxy-stats http://127.0.0.1:7200 -proxy-pids ${PIDS[0]} -redis-pids $RPIDS \
    -rate $RATE -duration $DUR -skew $s -label "skew plain-proxy" -out $OUT > /dev/null
  stop_proxies
  proxy 7100 7200 -hot=true
  ./bin/hkload -mode proxy -proxies 127.0.0.1:7100 -proxy-stats http://127.0.0.1:7200 -proxy-pids ${PIDS[0]} -redis-pids $RPIDS \
    -rate $RATE -duration $DUR -skew $s -label "skew hot-proxy" -out $OUT > /dev/null
  stop_proxies
done
done

echo "== E2: limited fan-out, 4 proxies with small caches"
for g in 1 2 4; do
  for i in 0 1 2 3; do proxy $((7100+i)) $((7200+i)) -hot=true -cache 64 -threshold 100; done
  ./bin/hkload -mode proxy -groups $g -proxies 127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103 \
    -proxy-stats http://127.0.0.1:7200,http://127.0.0.1:7201,http://127.0.0.1:7202,http://127.0.0.1:7203 \
    -proxy-pids $(IFS=,; echo "${PIDS[*]}") -rate $RATE -duration $DUR -skew 1.2 -rooms 100000 -label "fanout" -out $OUT > /dev/null
  stop_proxies
done

echo "== E3: viral-room detection delay (cold room swaps with the top room at t=10s)"
for i in 1 2 3; do
  proxy 7100 7200 -hot=true
  ./bin/hkload -mode proxy -proxies 127.0.0.1:7100 -proxy-stats http://127.0.0.1:7200 -rate $RATE -duration $DUR -skew 1.2 \
    -shift-at 10s -label "detect" -out $OUT > /dev/null
  stop_proxies
done

echo "== E4: write combining window (skew 2.0, hot writes concentrated on few rooms)"
for rep in 1 2; do
for w in 0 500us 2ms; do
  proxy 7100 7200 -hot=true -combine $w
  ./bin/hkload -mode proxy -proxies 127.0.0.1:7100 -proxy-stats http://127.0.0.1:7200 -proxy-pids ${PIDS[0]} -redis-pids $RPIDS \
    -rate $RATE -duration $DUR -skew 2.0 -label "combine $w" -out $OUT > /dev/null
  stop_proxies
done
done

fi

if [[ $PHASE == all || $PHASE == more || $PHASE == e5 ]]; then
echo "== E5: tenant isolation. Victim: live-room mix at 2,000 rps. Noisy: top-200 leaderboard reads at"
echo "   300 rps (about 600 RU/s, quota 600), bursting to 10x for 10 s. Caching off so every read reaches Redis."
echo "   off: no isolation; soft: WFQ per shard, over-quota requests at 1/10 weight; shape: quota enforced."
for rep in 1 2 3; do
  for mode in off soft shape; do
    case $mode in
      off) iso="-isolation=false" ;;
      soft) iso="-isolation=true" ;;
      shape) iso="-isolation=true -quota-shape -max-quota-wait 200ms" ;;
    esac
    proxy 7100 7200 -hot=false $iso -max-inflight 8 -tenants victim:4000:1,noisy:600:1
    ./bin/hkload -mode proxy -tenant noisy -mix heavy -heavy-rooms 100 -preload-lb 1000 -proxies 127.0.0.1:7100 \
      -rate 300 -duration 30s -burst-at 10s -burst-dur 10s -burst-x 10 -label "isolation=$mode noisy" -out $OUT > /dev/null &
    NP=$!
    sleep 2  # preload done before the victim starts measuring
    ./bin/hkload -mode proxy -tenant victim -flush=false -proxies 127.0.0.1:7100 -proxy-stats http://127.0.0.1:7200 -proxy-pids ${PIDS[0]} \
      -redis-pids $RPIDS -rate 2000 -duration 28s -skew 1.2 -window 8s:18s -label "isolation=$mode victim" -out $OUT > /dev/null
    wait $NP
    curl -s 127.0.0.1:7200 | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps({'label':'isolation=$mode tenants','tenants':d.get('tenants'),'fair_queued':d.get('fair_queued')}))" >> $OUT
    stop_proxies
  done
done
fi

if [[ $PHASE == all || $PHASE == more ]]; then
echo "== E6: Redis client-side caching (CLIENT TRACKING, 4 application servers) vs direct and the hot-key proxy"
for rep in 1 2 3; do
for s in 1.2 2.0; do
  ./bin/hkload -mode tracking -tracking-clients 4 -rate $RATE -duration $DUR -skew $s -redis-pids $RPIDS -label "tracking" -out $OUT > /dev/null
  proxy 7100 7200 -hot=true
  ./bin/hkload -mode proxy -proxies 127.0.0.1:7100 -proxy-stats http://127.0.0.1:7200 -proxy-pids ${PIDS[0]} -redis-pids $RPIDS \
    -rate $RATE -duration $DUR -skew $s -label "tracking-compare hot-proxy" -out $OUT > /dev/null
  stop_proxies
done
done

echo "== E7: throughput per proxy core (proxy pinned to CPU 1 with GOMAXPROCS=1; Redis and the load generator on CPU 0)"
for p in ${RPIDS//,/ }; do taskset -cp 0 $p > /dev/null; done
for hot in false true; do
  for r in 20000 40000 60000 80000; do
    GOMAXPROCS=1 taskset -c 1 ./bin/hkproxy -listen 127.0.0.1:7100 -stats 127.0.0.1:7200 -hot=$hot > /tmp/hkproxy-7100.log 2>&1 &
    PIDS+=($!)
    until curl -sf 127.0.0.1:7200 >/dev/null; do sleep 0.1; done
    taskset -c 0 ./bin/hkload -mode proxy -proxies 127.0.0.1:7100 -proxy-stats http://127.0.0.1:7200 -proxy-pids ${PIDS[0]} -redis-pids $RPIDS \
      -rate $r -duration 15s -skew 1.2 -workers 512 -conns 8 -label "percore hot=$hot" -out $OUT > /dev/null
    stop_proxies
  done
done
for p in ${RPIDS//,/ }; do taskset -cp 0-1 $p > /dev/null; done
fi

python3 bench/plot.py $OUT results
