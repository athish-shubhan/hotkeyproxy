// Package backend multiplexes many concurrent requests onto a few pipelined
// connections per Redis server: a writer goroutine batches encoded commands
// into one write, a reader goroutine matches replies to callers in FIFO
// order (Redis answers each connection in order).
package backend

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"hotkeyproxy/resp"
)

type Result struct {
	Reply resp.Reply
	Err   error
}

type call struct {
	cmd  []byte
	done chan Result
}

type conn struct {
	addr string
	in   chan *call
	init []byte // sent first on every new connection (AUTH, CLIENT TRACKING ...); its reply is dropped
	quit chan struct{}
	// readTimeout bounds the wait for a reply while any is outstanding; 0 = none
	readTimeout time.Duration
}

// Server is a pool of pipelined connections to one Redis server.
type Server struct {
	Addr   string
	conns  []*conn
	next   atomic.Uint64
	Calls  atomic.Int64
	quit   chan struct{}
	mu     sync.RWMutex // held for reading while queueing, for writing by Close
	closed bool
}

func NewServer(addr string, nconns int) *Server { return NewServerInit(addr, nconns, nil) }

// NewServerInit is NewServer with a command sent first on each connection.
// nconns below 1 is treated as 1.
func NewServerInit(addr string, nconns int, init []byte) *Server {
	return NewServerOptions(addr, nconns, Options{Init: init})
}

// Options configure a Server.
type Options struct {
	Init []byte // sent first on every new connection; its reply is dropped
	// ReadTimeout is how long a connection may wait for the next reply while
	// it has requests outstanding. When it passes, the connection is closed,
	// its outstanding calls fail with an error wrapping ErrTimeout, and it
	// reconnects. Without it a server that stops answering (stopped
	// process, half-open TCP connection) blocks every caller forever. 0 means
	// no limit. An idle connection is never timed out.
	ReadTimeout time.Duration
}

// NewServerOptions is NewServer with options. nconns below 1 is treated as 1.
func NewServerOptions(addr string, nconns int, o Options) *Server {
	s := &Server{Addr: addr, quit: make(chan struct{})}
	for i := 0; i < max(nconns, 1); i++ {
		c := &conn{addr: addr, in: make(chan *call, 4096), init: o.Init, quit: s.quit, readTimeout: o.ReadTimeout}
		s.conns = append(s.conns, c)
		go c.loop()
	}
	return s
}

// Do sends an encoded command and waits for its reply.
func (s *Server) Do(cmd []byte) (resp.Reply, error) {
	ch := s.Go(cmd)
	r := <-ch
	return r.Reply, r.Err
}

// Go sends an encoded command and returns a channel for its single reply.
// cmd must not be modified until the reply arrives. After Close the reply
// is an error.
func (s *Server) Go(cmd []byte) <-chan Result {
	c := &call{cmd: cmd, done: make(chan Result, 1)}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		c.done <- Result{Err: ErrServerClosed}
		return c.done
	}
	s.Calls.Add(1)
	// Queued before Close takes the lock, so the connection loop either
	// serves it or fails it when it drains on exit.
	s.conns[s.next.Add(1)%uint64(len(s.conns))].in <- c
	return c.done
}

// Close stops reconnecting and closes the connections. Calls queued or in
// flight fail with an error; later calls fail with ErrServerClosed. It is
// safe to call more than once.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.quit)
	}
}

var (
	errClosed = errors.New("backend connection lost")
	// ErrServerClosed is returned for calls made on or pending at Close.
	ErrServerClosed = errors.New("backend server closed")
	// ErrTimeout is wrapped by the error of calls that were outstanding when
	// a reply took longer than Options.ReadTimeout.
	ErrTimeout = errors.New("backend reply timeout")
)

func (c *conn) loop() {
	backoff := 50 * time.Millisecond
	for {
		select {
		case <-c.quit:
			drain(c.in, ErrServerClosed)
			return
		default:
		}
		nc, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
		if err != nil {
			// fail whatever is queued, then retry the connection
			drain(c.in, err)
			select {
			case <-time.After(backoff):
			case <-c.quit:
			}
			if backoff < time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 50 * time.Millisecond
		c.serve(nc)
	}
}

func drain(in chan *call, err error) {
	for {
		select {
		case cl := <-in:
			cl.done <- Result{Err: err}
		default:
			return
		}
	}
}

func (c *conn) serve(nc net.Conn) {
	pending := make(chan *call, 65536)
	done := make(chan struct{})
	var once sync.Once
	lost := errClosed // why the connection ended; written before done is closed
	stop := func(err error) {
		once.Do(func() {
			if err != nil {
				lost = err
			}
			close(done)
			_ = nc.Close()
		})
	}
	go func() { // Close: unblock the reader and the writer
		select {
		case <-c.quit:
			stop(nil)
		case <-done:
		}
	}()
	// Reply timeout: a watchdog closes the connection when replies are
	// outstanding and none has arrived for readTimeout. The hot path only
	// counts and stamps (two atomic stores per reply); a per-reply
	// SetReadDeadline cost about 5% of proxy CPU in benchmark E1.
	var sent, recv, progress atomic.Int64 // progress: unix nanos of the last reply, or of leaving idle
	if c.readTimeout > 0 {
		go func() {
			tick := time.NewTicker(max(c.readTimeout/4, 5*time.Millisecond))
			defer tick.Stop()
			for {
				select {
				case <-done:
					return
				case now := <-tick.C:
					if sent.Load() > recv.Load() && now.UnixNano()-progress.Load() > int64(c.readTimeout) {
						stop(fmt.Errorf("%w after %v (%s)", ErrTimeout, c.readTimeout, c.addr))
						return
					}
				}
			}
		}()
	}
	queued := func(n int64) {
		if recv.Load() == sent.Load() { // leaving idle: the clock starts now
			progress.Store(time.Now().UnixNano())
		}
		sent.Add(n)
	}
	// reader
	go func() {
		r := resp.NewReader(nc)
		for {
			rep, err := r.ReadReply()
			if err != nil {
				stop(nil)
				return
			}
			if c.readTimeout > 0 {
				progress.Store(time.Now().UnixNano())
				recv.Add(1)
			}
			select {
			case cl := <-pending:
				cl.done <- Result{Reply: rep}
			case <-done:
				return
			}
		}
	}()
	w := bufio.NewWriterSize(nc, 64<<10)
	// bufio.Writer keeps the first error and returns it from Flush, so the
	// individual Write errors can be ignored here.
	if c.init != nil {
		pending <- &call{cmd: c.init, done: make(chan Result, 1)}
		_, _ = w.Write(c.init)
		if c.readTimeout > 0 {
			queued(1)
		}
	}
	for {
		var cl *call
		select {
		case cl = <-c.in:
		case <-done:
			drain(pending, lost)
			return
		}
		pending <- cl
		_, _ = w.Write(cl.cmd)
		n := int64(1)
		// batch everything already queued into one write
		for more := true; more; {
			select {
			case cl = <-c.in:
				pending <- cl
				_, _ = w.Write(cl.cmd)
				n++
			default:
				more = false
			}
		}
		if c.readTimeout > 0 {
			queued(n)
		}
		if err := w.Flush(); err != nil {
			stop(nil)
			<-done
			drain(pending, lost)
			return
		}
	}
}
