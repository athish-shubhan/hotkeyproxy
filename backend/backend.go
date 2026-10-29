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
}

// Server is a pool of pipelined connections to one Redis server.
type Server struct {
	Addr  string
	conns []*conn
	next  atomic.Uint64
	Calls atomic.Int64
}

func NewServer(addr string, nconns int) *Server { return NewServerInit(addr, nconns, nil) }

// NewServerInit is NewServer with a command sent first on each connection.
func NewServerInit(addr string, nconns int, init []byte) *Server {
	s := &Server{Addr: addr}
	for i := 0; i < nconns; i++ {
		c := &conn{addr: addr, in: make(chan *call, 4096), init: init}
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
// cmd must not be modified until the reply arrives.
func (s *Server) Go(cmd []byte) <-chan Result {
	s.Calls.Add(1)
	c := &call{cmd: cmd, done: make(chan Result, 1)}
	s.conns[s.next.Add(1)%uint64(len(s.conns))].in <- c
	return c.done
}

var errClosed = errors.New("backend connection lost")

func (c *conn) loop() {
	backoff := 50 * time.Millisecond
	for {
		nc, err := net.DialTimeout("tcp", c.addr, 2*time.Second)
		if err != nil {
			// fail whatever is queued, then retry the connection
			drain(c.in, err)
			time.Sleep(backoff)
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
	stop := func() { once.Do(func() { close(done); nc.Close() }) }
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
	if c.init != nil {
		pending <- &call{cmd: c.init, done: make(chan Result, 1)}
		w.Write(c.init)
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
		w.Write(cl.cmd)
		// batch everything already queued into one write
		for more := true; more; {
			select {
			case cl = <-c.in:
				pending <- cl
				w.Write(cl.cmd)
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
