package resp

import (
	"bytes"
	"strings"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	var b []byte
	b = AppendCommand(b, []byte("SET"), []byte("k"), []byte("v\r\nwith crlf"))
	b = AppendCommand(b, []byte("GET"), []byte("k"))
	r := NewReader(bytes.NewReader(b))
	a, err := r.ReadCommand()
	if err != nil || len(a) != 3 || string(a[2]) != "v\r\nwith crlf" {
		t.Fatalf("%q %v", a, err)
	}
	a, err = r.ReadCommand()
	if err != nil || len(a) != 2 || string(a[0]) != "GET" {
		t.Fatalf("%q %v", a, err)
	}
}

func TestInlineCommand(t *testing.T) {
	r := NewReader(strings.NewReader("PING  hello\r\n"))
	a, err := r.ReadCommand()
	if err != nil || len(a) != 2 || string(a[1]) != "hello" {
		t.Fatalf("%q %v", a, err)
	}
}

func TestNestedReplyKeepsRaw(t *testing.T) {
	wire := "*3\r\n$3\r\nfoo\r\n:42\r\n*2\r\n$-1\r\n-ERR x\r\n"
	r := NewReader(strings.NewReader(wire))
	rep, err := r.ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	if string(rep.Raw) != wire || len(rep.Elems) != 3 || rep.Elems[1].Int != 42 || rep.Elems[2].Elems[0].Str != nil {
		t.Fatalf("%+v", rep)
	}
}

func TestReadCommandAllocations(t *testing.T) {
	var b []byte
	for i := 0; i < 200; i++ {
		b = AppendCommand(b, []byte("GET"), []byte("room:1:viewers"))
	}
	r := NewReader(bytes.NewReader(b))
	r.ReadCommand() // warm the buffers
	allocs := testing.AllocsPerRun(100, func() { r.ReadCommand() })
	if allocs > 0 {
		t.Fatalf("ReadCommand allocates %.1f times per call", allocs)
	}
}

func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$-5\r\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewReader(bytes.NewReader(b))
		for i := 0; i < 4; i++ {
			if _, err := r.ReadCommand(); err != nil {
				return
			}
		}
	})
}
