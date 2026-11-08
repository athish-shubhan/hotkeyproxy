package cluster

import (
	"bufio"
	"fmt"
	"strings"
	"testing"

	"hotkeyproxy/resp"
)

func TestCRC16AndSlots(t *testing.T) {
	if c := CRC16([]byte("123456789")); c != 0x31C3 {
		t.Fatalf("CRC16 check value %#x, want 0x31c3", c)
	}
	// known Redis values (CLUSTER KEYSLOT)
	for k, want := range map[string]int{"foo": 12182, "bar": 5061, "hello": 866} {
		if s := Slot([]byte(k)); s != want {
			t.Fatalf("slot(%s)=%d want %d", k, s, want)
		}
	}
	if Slot([]byte("{user1000}.following")) != Slot([]byte("{user1000}.followers")) {
		t.Fatal("hash tags not honoured")
	}
	// An empty tag "{}" is ignored and the whole key is hashed (Redis rule:
	// only the first '{' counts, and it needs a non-empty tag).
	if Slot([]byte("foo{}{bar}")) != int(CRC16([]byte("foo{}{bar}")))%Slots || Slot([]byte("foo{}{bar}")) == Slot([]byte("bar")) {
		t.Fatal("empty hash tag must hash the whole key")
	}
	if Slot([]byte("{bar}x")) != Slot([]byte("bar")) || Slot([]byte("x{bar")) != int(CRC16([]byte("x{bar")))%Slots {
		t.Fatal("tag rules")
	}
}

// slotsReply builds a CLUSTER SLOTS reply from "lo-hi host:port" entries.
func slotsReply(t *testing.T, entries ...string) resp.Reply {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(entries))
	for _, e := range entries {
		var lo, hi int
		var hp string
		if _, err := fmt.Sscanf(e, "%d-%d %s", &lo, &hi, &hp); err != nil {
			t.Fatal(err)
		}
		i := strings.LastIndexByte(hp, ':')
		host, port := hp[:i], hp[i+1:]
		fmt.Fprintf(&b, "*3\r\n:%d\r\n:%d\r\n*2\r\n$%d\r\n%s\r\n:%s\r\n", lo, hi, len(host), host, port)
	}
	rep, err := resp.NewReader(bufio.NewReader(strings.NewReader(b.String()))).ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestFromClusterSlots(t *testing.T) {
	m, err := FromClusterSlots(slotsReply(t, "0-5460 10.0.0.1:7001", "5461-10922 10.0.0.2:7002", "10923-16383 10.0.0.1:7001"))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Addrs) != 2 || m.Addrs[0] != "10.0.0.1:7001" || m.Addrs[1] != "10.0.0.2:7002" {
		t.Fatalf("addrs %v (one entry per master, first-seen order)", m.Addrs)
	}
	if m.Owner[0] != 0 || m.Owner[5461] != 1 || m.Owner[16383] != 0 {
		t.Fatalf("owners: %d %d %d", m.Owner[0], m.Owner[5461], m.Owner[16383])
	}
	if m.Shard([]byte("foo")) != m.Owner[12182] {
		t.Fatal("Shard must use the key's slot")
	}
}

func TestFromClusterSlotsHosts(t *testing.T) {
	m, err := FromClusterSlots(slotsReply(t, "0-16383 ::1:7001"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Addrs[0] != "[::1]:7001" {
		t.Fatalf("IPv6 address %q, want [::1]:7001", m.Addrs[0])
	}
	m, err = FromClusterSlots(slotsReply(t, "0-16383 :7001"))
	if err != nil || m.Addrs[0] != "127.0.0.1:7001" {
		t.Fatalf("empty host: %v %v", m, err)
	}
}

func TestFromClusterSlotsRejectsBadReplies(t *testing.T) {
	cases := map[string]resp.Reply{
		"uncovered slot":  slotsReply(t, "0-100 h:1"),
		"negative start":  slotsReply(t, "-1-16383 h:1"), // used to panic
		"end past 16383":  slotsReply(t, "0-16384 h:1"),
		"start after end": slotsReply(t, "10-5 h:1", "0-16383 h:2"),
		"bad port":        slotsReply(t, "0-16383 h:0"),
		"not an array":    {Type: '+', Raw: []byte("+OK\r\n")},
	}
	for name, rep := range cases {
		if _, err := FromClusterSlots(rep); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
