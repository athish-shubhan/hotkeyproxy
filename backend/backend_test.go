package backend

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hotkeyproxy/resp"
)

// echoServer answers every command with a bulk string of its arguments
// joined by spaces, in order, so a caller can check it got its own reply.
// dropAfter > 0 closes each connection after that many commands.
type echoServer struct {
	ln        net.Listener
	conns     atomic.Int64
	cmds      atomic.Int64
	dropAfter int64
	mu        sync.Mutex
	first     []string // first command seen on each connection
	// a command named HANG gets no reply, and nothing after it on that
	// connection is answered (a stopped or wedged server)
}

func startEcho(t *testing.T, dropAfter int64) *echoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &echoServer{ln: ln, dropAfter: dropAfter}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			e.conns.Add(1)
			go e.serve(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return e
}

func (e *echoServer) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := resp.NewReader(c)
	w := bufio.NewWriter(c)
	var n int64
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		var parts []string
		for _, a := range args {
			parts = append(parts, string(a))
		}
		if n == 0 {
			e.mu.Lock()
			e.first = append(e.first, strings.Join(parts, " "))
			e.mu.Unlock()
		}
		n++
		e.cmds.Add(1)
		if parts[0] == "HANG" {
			_ = w.Flush()
			_, _ = io.Copy(io.Discard, c) // read and ignore until the client closes
			return
		}
		_, _ = w.Write(resp.AppendBulk(nil, []byte(strings.Join(parts, " "))))
		if r.Buffered() == 0 {
			if w.Flush() != nil {
				return
			}
		}
		if e.dropAfter > 0 && n >= e.dropAfter {
			_ = w.Flush()
			return
		}
	}
}

func cmd(args ...string) []byte {
	var b [][]byte
	for _, a := range args {
		b = append(b, []byte(a))
	}
	return resp.AppendCommand(nil, b...)
}

// Many concurrent callers on a few pipelined connections each get their
// own reply (FIFO matching per connection).
func TestPipelinedRepliesMatchCallers(t *testing.T) {
	e := startEcho(t, 0)
	s := NewServer(e.ln.Addr().String(), 3)
	defer s.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 2000)
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				want := fmt.Sprintf("GET k%d-%d", g, i)
				rep, err := s.Do(cmd("GET", fmt.Sprintf("k%d-%d", g, i)))
				if err != nil {
					errs <- err
					return
				}
				if string(rep.Str) != want {
					errs <- fmt.Errorf("got %q, want %q", rep.Str, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := s.Calls.Load(); got != 2000 {
		t.Fatalf("Calls = %d, want 2000", got)
	}
	if c := e.conns.Load(); c != 3 {
		t.Fatalf("connections = %d, want 3", c)
	}
}

// The init command goes first on every connection and its reply is not
// handed to a caller.
func TestInitCommandSentFirstAndReplyDropped(t *testing.T) {
	e := startEcho(t, 0)
	s := NewServerInit(e.ln.Addr().String(), 1, cmd("CLIENT", "TRACKING", "on"))
	defer s.Close()
	rep, err := s.Do(cmd("PING"))
	if err != nil || string(rep.Str) != "PING" {
		t.Fatalf("first caller got %q %v, want its own PING reply", rep.Str, err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.first) != 1 || e.first[0] != "CLIENT TRACKING on" {
		t.Fatalf("first command per connection: %q", e.first)
	}
}

// When the server drops the connection, callers get an error or a reply
// (never a wrong reply or a hang), and the pool reconnects.
func TestReconnectAfterConnectionLoss(t *testing.T) {
	e := startEcho(t, 5)
	s := NewServer(e.ln.Addr().String(), 1)
	defer s.Close()
	ok, failed := 0, 0
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; ok < 20 && time.Now().Before(deadline); i++ {
		want := fmt.Sprint("ECHO ", i)
		rep, err := s.Do(cmd("ECHO", fmt.Sprint(i)))
		if err != nil {
			failed++
			continue
		}
		if string(rep.Str) != want {
			t.Fatalf("got %q, want %q", rep.Str, want)
		}
		ok++
	}
	if ok < 20 {
		t.Fatalf("only %d successful calls (%d failed)", ok, failed)
	}
	if e.conns.Load() < 4 {
		t.Fatalf("expected reconnects, saw %d connections", e.conns.Load())
	}
}

func TestDialFailureReturnsError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens here now
	s := NewServer(addr, 1)
	defer s.Close()
	done := make(chan error, 1)
	go func() { _, err := s.Do(cmd("PING")); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("want a dial error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("call to an unreachable server hung")
	}
}

func TestCloseFailsCallsAndStopsReconnecting(t *testing.T) {
	e := startEcho(t, 0)
	s := NewServer(e.ln.Addr().String(), 2)
	if _, err := s.Do(cmd("PING")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s.Close() // idempotent
	if _, err := s.Do(cmd("PING")); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("after Close: %v", err)
	}
	before := e.conns.Load()
	time.Sleep(300 * time.Millisecond)
	if after := e.conns.Load(); after != before {
		t.Fatalf("reconnected after Close: %d -> %d connections", before, after)
	}
}

func TestZeroConnsMeansOne(t *testing.T) {
	e := startEcho(t, 0)
	s := NewServer(e.ln.Addr().String(), 0) // used to divide by zero in Go
	defer s.Close()
	if rep, err := s.Do(cmd("PING")); err != nil || string(rep.Str) != "PING" {
		t.Fatalf("%q %v", rep.Str, err)
	}
}

// A server that stops answering fails the outstanding calls with ErrTimeout
// after ReadTimeout instead of blocking them forever, and the pool recovers.
func TestReadTimeoutFailsOutstandingCalls(t *testing.T) {
	e := startEcho(t, 0)
	s := NewServerOptions(e.ln.Addr().String(), 1, Options{ReadTimeout: 200 * time.Millisecond})
	defer s.Close()
	if _, err := s.Do(cmd("PING")); err != nil {
		t.Fatal(err)
	}
	// idle longer than the timeout: an idle connection must not be closed
	time.Sleep(400 * time.Millisecond)
	if rep, err := s.Do(cmd("PING")); err != nil || string(rep.Str) != "PING" {
		t.Fatalf("after idling: %q %v", rep.Str, err)
	}
	conns := e.conns.Load()
	start := time.Now()
	hang := s.Go(cmd("HANG"))
	behind := s.Go(cmd("PING")) // queued behind it on the same connection
	for _, ch := range []<-chan Result{hang, behind} {
		select {
		case r := <-ch:
			if !errors.Is(r.Err, ErrTimeout) {
				t.Fatalf("want ErrTimeout, got %v", r.Err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("call to a server that stopped answering hung")
		}
	}
	if d := time.Since(start); d < 150*time.Millisecond || d > 2*time.Second {
		t.Fatalf("timed out after %v, want about 200ms", d)
	}
	if rep, err := s.Do(cmd("PING")); err != nil || string(rep.Str) != "PING" {
		t.Fatalf("after the timeout the pool should reconnect: %q %v", rep.Str, err)
	}
	if e.conns.Load() <= conns {
		t.Fatal("expected a new connection")
	}
}

// Without a ReadTimeout (the default for NewServer) behaviour is unchanged.
func TestNoReadTimeoutByDefault(t *testing.T) {
	e := startEcho(t, 0)
	s := NewServer(e.ln.Addr().String(), 1)
	defer s.Close()
	ch := s.Go(cmd("HANG"))
	select {
	case r := <-ch:
		t.Fatalf("returned %v without a timeout configured", r)
	case <-time.After(300 * time.Millisecond):
	}
}
