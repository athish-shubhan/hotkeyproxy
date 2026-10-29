// Package resp parses and writes the Redis serialization protocol (RESP2).
// The Reader parses commands (arrays of bulk strings) and replies without
// allocating per argument: argument slices alias an internal buffer that is
// reused by the next call, so callers copy what they keep.
package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

var (
	ErrProtocol = errors.New("resp: protocol error")
	ErrTooLarge = errors.New("resp: element too large")
)

const maxBulk = 512 << 20

type Reader struct {
	r    *bufio.Reader
	args [][]byte
	buf  []byte
	offs [][2]int
}

// grow extends buf by n bytes without a temporary allocation.
func grow(buf []byte, n int) []byte {
	need := len(buf) + n
	if cap(buf) < need {
		nb := make([]byte, len(buf), 2*need)
		copy(nb, buf)
		buf = nb
	}
	return buf[:need]
}

func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReaderSize(r, 64<<10)} }

// Buffered reports bytes already read from the connection but not parsed:
// a pipelining client has more commands waiting.
func (r *Reader) Buffered() int { return r.r.Buffered() }

func (r *Reader) line() ([]byte, error) {
	l, err := r.r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(l) < 3 || l[len(l)-2] != '\r' {
		return nil, ErrProtocol
	}
	return l[:len(l)-2], nil
}

func atoi(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, ErrProtocol
	}
	neg := false
	if b[0] == '-' {
		neg, b = true, b[1:]
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, ErrProtocol
		}
		n = n*10 + int(c-'0')
		if n > maxBulk {
			return 0, ErrTooLarge
		}
	}
	if neg {
		n = -n
	}
	return n, nil
}

// ReadCommand reads one command as a slice of arguments. Inline commands
// (plain text lines, as typed in telnet) are accepted too.
func (r *Reader) ReadCommand() ([][]byte, error) {
	l, err := r.line()
	if err != nil {
		return nil, err
	}
	r.args = r.args[:0]
	r.buf = r.buf[:0]
	if l[0] != '*' {
		// inline command: split on spaces
		start := -1
		var spans [][2]int
		for i := 0; i <= len(l); i++ {
			if i == len(l) || l[i] == ' ' {
				if start >= 0 {
					spans = append(spans, [2]int{start, i})
					start = -1
				}
			} else if start < 0 {
				start = i
			}
		}
		r.buf = append(r.buf, l...)
		for _, s := range spans {
			r.args = append(r.args, r.buf[s[0]:s[1]])
		}
		return r.args, nil
	}
	n, err := atoi(l[1:])
	if err != nil || n < 0 {
		return nil, ErrProtocol
	}
	r.offs = r.offs[:0]
	for i := 0; i < n; i++ {
		h, err := r.line()
		if err != nil {
			return nil, err
		}
		if len(h) < 2 || h[0] != '$' {
			return nil, ErrProtocol
		}
		sz, err := atoi(h[1:])
		if err != nil || sz < 0 {
			return nil, ErrProtocol
		}
		start := len(r.buf)
		r.buf = grow(r.buf, sz+2)
		if _, err := io.ReadFull(r.r, r.buf[start:start+sz+2]); err != nil {
			return nil, err
		}
		r.buf = r.buf[:start+sz]
		r.offs = append(r.offs, [2]int{start, start + sz})
	}
	for _, o := range r.offs {
		r.args = append(r.args, r.buf[o[0]:o[1]])
	}
	return r.args, nil
}

// Reply is a parsed reply. Raw holds the exact bytes on the wire, so a proxy
// can forward or cache it without re-encoding.
type Reply struct {
	Type  byte // '+', '-', ':', '$', '*'
	Int   int64
	Str   []byte // simple string, error, or bulk body (nil for null bulk)
	Elems []Reply
	Raw   []byte
}

// ReadReply reads one reply, including nested arrays, and returns it with
// its raw encoding (a fresh copy the caller owns).
func (r *Reader) ReadReply() (Reply, error) {
	raw := make([]byte, 0, 64)
	rep, raw, err := r.readReply(raw)
	rep.Raw = raw
	return rep, err
}

func (r *Reader) readReply(raw []byte) (Reply, []byte, error) {
	l, err := r.line()
	if err != nil {
		return Reply{}, raw, err
	}
	raw = append(raw, l...)
	raw = append(raw, '\r', '\n')
	rep := Reply{Type: l[0]}
	switch l[0] {
	case '+', '-':
		rep.Str = append([]byte(nil), l[1:]...)
	case ':':
		n, err := strconv.ParseInt(string(l[1:]), 10, 64)
		if err != nil {
			return rep, raw, ErrProtocol
		}
		rep.Int = n
	case '$':
		sz, err := atoi(l[1:])
		if err != nil {
			return rep, raw, err
		}
		if sz < 0 {
			return rep, raw, nil
		}
		start := len(raw)
		raw = grow(raw, sz+2)
		if _, err := io.ReadFull(r.r, raw[start:]); err != nil {
			return rep, raw, err
		}
		rep.Str = raw[start : start+sz]
	case '*':
		n, err := atoi(l[1:])
		if err != nil {
			return rep, raw, err
		}
		for i := 0; i < n; i++ {
			var e Reply
			e, raw, err = r.readReply(raw)
			if err != nil {
				return rep, raw, err
			}
			rep.Elems = append(rep.Elems, e)
		}
	default:
		return rep, raw, ErrProtocol
	}
	return rep, raw, nil
}

// AppendCommand encodes a command as an array of bulk strings.
func AppendCommand(dst []byte, args ...[]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

func AppendInt(dst []byte, n int64) []byte {
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, n, 10)
	return append(dst, '\r', '\n')
}

func AppendError(dst []byte, msg string) []byte {
	dst = append(dst, '-')
	dst = append(dst, msg...)
	return append(dst, '\r', '\n')
}

func AppendSimple(dst []byte, s string) []byte {
	dst = append(dst, '+')
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

func AppendBulk(dst []byte, b []byte) []byte {
	if b == nil {
		return append(dst, "$-1\r\n"...)
	}
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}
