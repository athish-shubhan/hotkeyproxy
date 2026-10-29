package main

// Redis client-side caching (CLIENT TRACKING) as a baseline: each simulated
// application server keeps a local cache of read replies, and Redis tells it
// when a key it has read changes. RESP2 redirect mode: per shard, one
// connection subscribes to __redis__:invalidate and the data connections
// enable CLIENT TRACKING on REDIRECT <that connection's id>.
//
// Race handling (the documented one for two-connection mode): every data key
// has an invalidation counter; a read stores its reply only if no
// invalidation for the key arrived between sending the read and getting the
// reply. A read can still return a value that a write overwrote a moment ago
// (until the invalidation message arrives): that is the staleness this
// design accepts, bounded by invalidation delivery, not by a timer.

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"hotkeyproxy/backend"
	"hotkeyproxy/cluster"
	"hotkeyproxy/resp"
)

type tcEntry struct {
	raw []byte
}

type trackingClient struct {
	m      *cluster.Map
	shards []*backend.Server
	mu     sync.RWMutex
	cache  map[string]tcEntry  // encoded read command -> reply
	byKey  map[string][]string // data key -> encoded commands cached for it
	inv    map[string]uint64   // data key -> invalidations seen
	Hits   atomic.Int64
	Misses atomic.Int64
	Invals atomic.Int64
	Flush  atomic.Int64
}

var trackCacheable = map[string]bool{"GET": true, "ZREVRANGE": true, "ZRANGE": true, "ZSCORE": true}

func newTrackingClient(m *cluster.Map, conns int) (*trackingClient, error) {
	tc := &trackingClient{m: m, cache: map[string]tcEntry{}, byKey: map[string][]string{}, inv: map[string]uint64{}}
	for _, addr := range m.Addrs {
		nc, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			return nil, err
		}
		r := resp.NewReader(bufio.NewReader(nc))
		nc.Write(resp.AppendCommand(nil, []byte("CLIENT"), []byte("ID")))
		idr, err := r.ReadReply()
		if err != nil {
			return nil, err
		}
		nc.Write(resp.AppendCommand(nil, []byte("SUBSCRIBE"), []byte("__redis__:invalidate")))
		if _, err := r.ReadReply(); err != nil {
			return nil, err
		}
		go tc.listen(r)
		init := resp.AppendCommand(nil, []byte("CLIENT"), []byte("TRACKING"), []byte("on"), []byte("REDIRECT"), []byte(strconv.FormatInt(idr.Int, 10)))
		tc.shards = append(tc.shards, backend.NewServerInit(addr, conns, init))
	}
	return tc, nil
}

// listen consumes invalidation messages: ["message", channel, [keys...]] (nil keys = flush everything).
func (tc *trackingClient) listen(r *resp.Reader) {
	for {
		rep, err := r.ReadReply()
		if err != nil {
			return
		}
		if len(rep.Elems) != 3 || string(rep.Elems[0].Str) != "message" {
			continue
		}
		keys := rep.Elems[2]
		tc.mu.Lock()
		if keys.Type == '*' && len(keys.Elems) > 0 {
			for _, k := range keys.Elems {
				ks := string(k.Str)
				tc.inv[ks]++
				for _, c := range tc.byKey[ks] {
					delete(tc.cache, c)
				}
				delete(tc.byKey, ks)
				tc.Invals.Add(1)
			}
		} else {
			tc.cache, tc.byKey = map[string]tcEntry{}, map[string][]string{}
			tc.Flush.Add(1)
		}
		tc.mu.Unlock()
	}
}

// do runs one command, from the local cache when it can.
func (tc *trackingClient) do(cmd string, key []byte, enc []byte) backend.Result {
	sh := tc.shards[tc.m.Shard(key)]
	if !trackCacheable[cmd] {
		return <-sh.Go(enc)
	}
	ck, ks := string(enc), string(key)
	tc.mu.RLock()
	e, ok := tc.cache[ck]
	seen := tc.inv[ks]
	tc.mu.RUnlock()
	if ok {
		tc.Hits.Add(1)
		return backend.Result{Reply: resp.Reply{Raw: e.raw, Type: e.raw[0]}}
	}
	tc.Misses.Add(1)
	res := <-sh.Go(enc)
	if res.Err == nil && res.Reply.Type != '-' {
		tc.mu.Lock()
		if tc.inv[ks] == seen { // no invalidation raced the read
			if _, dup := tc.cache[ck]; !dup {
				tc.byKey[ks] = append(tc.byKey[ks], ck)
			}
			tc.cache[ck] = tcEntry{raw: append([]byte(nil), res.Reply.Raw...)}
		}
		tc.mu.Unlock()
	}
	return res
}

func (tc *trackingClient) String() string {
	return fmt.Sprintf("hits=%d misses=%d invalidations=%d", tc.Hits.Load(), tc.Misses.Load(), tc.Invals.Load())
}
