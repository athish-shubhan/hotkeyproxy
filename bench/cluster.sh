#!/usr/bin/env bash
# Start (or stop) a local Redis Cluster of N masters on ports 7001..700N.
set -euo pipefail
N=${N:-3}
DIR=${DIR:-/tmp/hkp-cluster}
case "${1:-start}" in
start)
  rm -rf "$DIR"; mkdir -p "$DIR"
  ADDRS=""
  for i in $(seq 1 "$N"); do
    p=$((7000 + i)); mkdir -p "$DIR/$p"
    redis-server --port $p --cluster-enabled yes --cluster-config-file "$DIR/$p/nodes.conf" --dir "$DIR/$p" \
      --save "" --appendonly no --daemonize yes --logfile "$DIR/$p/log" --io-threads 1
    ADDRS="$ADDRS 127.0.0.1:$p"
  done
  sleep 0.5
  redis-cli --cluster create $ADDRS --cluster-replicas 0 --cluster-yes > "$DIR/create.log"
  until redis-cli -p 7001 cluster info | grep -q cluster_state:ok; do sleep 0.2; done
  echo "cluster up:$ADDRS"
  ;;
stop)
  for i in $(seq 1 "$N"); do redis-cli -p $((7000 + i)) shutdown nosave 2>/dev/null || true; done
  ;;
esac
