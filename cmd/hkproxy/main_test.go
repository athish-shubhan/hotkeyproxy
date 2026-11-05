package main

import (
	"testing"

	"hotkeyproxy/proxy"
)

func TestParseTenants(t *testing.T) {
	got, err := parseTenants("victim:4000:1, noisy:600:1,")
	if err != nil {
		t.Fatal(err)
	}
	want := []proxy.TenantConfig{{Name: "victim", QuotaRU: 4000, Weight: 1}, {Name: "noisy", QuotaRU: 600, Weight: 1}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v", got)
	}
	if ts, err := parseTenants(""); err != nil || len(ts) != 0 {
		t.Fatalf("empty spec: %v %v", ts, err)
	}
	for _, bad := range []string{"victim:4000", "victim:4OO:1", "victim:4000:0", "victim:-1:1", ":1:1", "a:1:1,a:2:1", "a:1:1:extra"} {
		if _, err := parseTenants(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestDefaultConfigValid(t *testing.T) {
	if err := proxy.DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	c := proxy.DefaultConfig()
	c.ConnsPerShard = 0
	if c.Validate() == nil {
		t.Fatal("-conns 0 must be rejected")
	}
	c = proxy.DefaultConfig()
	c.QuotaShape = true
	if c.Validate() == nil {
		t.Fatal("-quota-shape without -isolation must be rejected")
	}
	c = proxy.DefaultConfig()
	c.Sample = 0
	if c.Validate() == nil {
		t.Fatal("-sample 0 must be rejected")
	}
}
