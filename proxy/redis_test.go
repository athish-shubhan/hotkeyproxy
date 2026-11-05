package proxy

import (
	"bufio"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"hotkeyproxy/resp"
)

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

// client is a minimal RESP client for the tests.
type client struct {
	t *testing.T
	c net.Conn
	r *resp.Reader
	w *bufio.Writer
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	var c net.Conn
	var err error
	for i := 0; i < 100; i++ {
		if c, err = net.Dial("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &client{t, c, resp.NewReader(c), bufio.NewWriter(c)}
}

func (c *client) do(args ...string) resp.Reply {
	c.t.Helper()
	var b [][]byte
	for _, a := range args {
		b = append(b, []byte(a))
	}
	if _, err := c.w.Write(resp.AppendCommand(nil, b...)); err != nil {
		c.t.Fatal(err)
	}
	if err := c.w.Flush(); err != nil {
		c.t.Fatal(err)
	}
	rep, err := c.r.ReadReply()
	if err != nil {
		c.t.Fatal(err)
	}
	return rep
}

// startRedisCluster runs one redis-server in cluster mode that owns every
// slot, or skips the test when redis-server is not installed.
func startRedisCluster(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server not installed")
	}
	dir := t.TempDir()
	port := freePort(t)
	cmd := exec.Command(bin, "--port", strconv.Itoa(port), "--bind", "127.0.0.1", "--cluster-enabled", "yes",
		"--cluster-config-file", filepath.Join(dir, "nodes.conf"), "--dir", dir, "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	c := dial(t, addr)
	if r := c.do("CLUSTER", "ADDSLOTSRANGE", "0", "16383"); r.Type == '-' {
		t.Fatalf("ADDSLOTSRANGE: %s", r.Raw)
	}
	for i := 0; ; i++ {
		if strings.Contains(string(c.do("CLUSTER", "INFO").Str), "cluster_state:ok") {
			break
		}
		if i > 100 {
			t.Fatal("cluster did not become ok")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return addr
}

// End to end against a real Redis Cluster node: slot map from the real
// CLUSTER SLOTS reply, reads and writes through the proxy, hot-key caching,
// no lost increments under concurrency, CROSSSLOT checks.
func TestAgainstRealRedis(t *testing.T) {
	seed := startRedisCluster(t)
	cfg := DefaultConfig()
	cfg.Seed = seed
	cfg.Listen = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg.StatsAddr = ""
	cfg.HotThreshold = 5
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.slots.Addrs) != 1 || p.slots.Addrs[0] != seed {
		t.Fatalf("slot map from real CLUSTER SLOTS: %v, want [%s]", p.slots.Addrs, seed)
	}
	go func() { _ = p.ListenAndServe() }()
	c := dial(t, cfg.Listen)

	if r := c.do("PING"); string(r.Raw) != "+PONG\r\n" {
		t.Fatalf("PING: %q", r.Raw)
	}
	if r := c.do("SET", "room:1:title", "hello"); string(r.Raw) != "+OK\r\n" {
		t.Fatalf("SET: %q", r.Raw)
	}
	if r := c.do("GET", "room:1:title"); string(r.Str) != "hello" {
		t.Fatalf("GET: %q", r.Raw)
	}

	// 8 connections x 250 INCRs on one hot key: none may be lost
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc := dial(t, cfg.Listen)
			for i := 0; i < 250; i++ {
				if r := cc.do("INCR", "room:1:likes"); r.Type != ':' {
					t.Errorf("INCR: %q", r.Raw)
					return
				}
			}
		}()
	}
	wg.Wait()
	direct := dial(t, seed)
	if r := direct.do("GET", "room:1:likes"); string(r.Str) != "2000" {
		t.Fatalf("likes in Redis = %q, want 2000", r.Str)
	}

	// a hot key's reads are served from the cache and stay within the bound
	for i := 0; i < 50; i++ {
		c.do("GET", "room:1:title")
	}
	if p.S.CacheHits.Load() == 0 {
		t.Fatal("expected cache hits on a hot key")
	}
	direct.do("SET", "room:1:title", "updated")
	time.Sleep(cfg.Staleness + 100*time.Millisecond)
	if r := c.do("GET", "room:1:title"); string(r.Str) != "updated" {
		t.Fatalf("after the staleness bound the proxy must serve the new value, got %q", r.Str)
	}

	if r := c.do("MGET", "a", "b"); !strings.HasPrefix(string(r.Raw), "-CROSSSLOT") {
		t.Fatalf("MGET across slots: %q", r.Raw)
	}
	if r := c.do("MGET", "{r}a", "{r}b"); r.Type != '*' {
		t.Fatalf("MGET same slot: %q", r.Raw)
	}
	if r := c.do("MULTI"); r.Type != '-' {
		t.Fatalf("MULTI must be refused: %q", r.Raw)
	}
}
