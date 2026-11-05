// Package backend multiplexes many concurrent requests onto a few pipelined
// connections per Redis server: a writer goroutine batches encoded commands
// into one write, a reader goroutine matches replies to callers in FIFO
// order (Redis answers each connection in order).
package backend

import (
	"bufio"
	"errors"
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
	s := &Server{Addr: addr, quit: make(chan struct{})}
	for i := 0; i < max(nconns, 1); i++ {
		c := &conn{addr: addr, in: make(chan *call, 4096), init: init, quit: s.quit}
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
	stop := func() { once.Do(func() { close(done); _ = nc.Close() }) }
	go func() { // Close: unblock the reader and the writer
		select {
		case <-c.quit:
			stop()
		case <-done:
		}
	}()
	// reader
	go func() {
		r := resp.NewReader(nc)
		for {
			rep, err := r.ReadReply()
			if err != nil {
				stop()
				return
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
	}
	for {
		var cl *call
		select {
		case cl = <-c.in:
		case <-done:
			drain(pending, errClosed)
			return
		}
		pending <- cl
		_, _ = w.Write(cl.cmd)
		// batch everything already queued into one write
		for more := true; more; {
			select {
			case cl = <-c.in:
				pending <- cl
				_, _ = w.Write(cl.cmd)
			default:
				more = false
			}
		}
		if err := w.Flush(); err != nil {
			stop()
			drain(pending, errClosed)
			return
		}
	}
}
