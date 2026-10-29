package cluster

import "testing"

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
	if Slot([]byte("foo{}{bar}")) != Slot([]byte("foo{}{bar}")) || Slot([]byte("foo{}{bar}")) == Slot([]byte("bar")) {
		t.Fatal("empty hash tag must hash the whole key")
	}
}
