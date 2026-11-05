package main

import (
	"errors"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"hotkeyproxy/resp"
)

// The sampler draws room i with probability (1/(i+1)^s) / H(n, s).
func TestZipfMatchesItsDistribution(t *testing.T) {
	for _, s := range []float64{0.99, 1.2, 2.0} {
		const n, draws = 50, 200000
		z := newZipf(n, s)
		r := rand.New(rand.NewPCG(1, uint64(s*100)))
		count := make([]int, n)
		for i := 0; i < draws; i++ {
			k := z.sample(r)
			if k < 0 || k >= n {
				t.Fatalf("sample %d out of range", k)
			}
			count[k]++
		}
		var h float64
		for i := 1; i <= n; i++ {
			h += 1 / math.Pow(float64(i), s)
		}
		for _, i := range []int{0, 1, 4, 20} {
			p := 1 / math.Pow(float64(i+1), s) / h
			got := float64(count[i]) / draws
			se := math.Sqrt(p * (1 - p) / draws)
			if math.Abs(got-p) > 5*se {
				t.Errorf("s=%v room %d: share %.4f, want %.4f", s, i, got, p)
			}
		}
	}
}

func TestPct(t *testing.T) {
	var d []time.Duration
	for i := 1; i <= 1000; i++ {
		d = append(d, time.Duration(i)*time.Millisecond)
	}
	got := pct(d)
	// lower nearest rank: index floor(p*(n-1))
	if got != [3]float64{500, 990, 999} {
		t.Fatalf("%v", got)
	}
	if pct(nil) != [3]float64{} {
		t.Fatal("empty")
	}
}

func TestParseCommandstats(t *testing.T) {
	info := "# Commandstats\r\n" +
		"cmdstat_get:calls=120,usec=300,usec_per_call=2.50,rejected_calls=0,failed_calls=0\r\n" +
		"cmdstat_incrby:calls=30,usec=90,usec_per_call=3.00\r\n" +
		"cmdstat_info:calls=7,usec=700,usec_per_call=100.00\r\n" +
		"cmdstat_cluster|slots:calls=2,usec=20,usec_per_call=10.00\r\n" +
		"cmdstat_infox:calls=5,usec=1,usec_per_call=0.2\r\n"
	if got := parseCommandstats(info); got != 155 {
		t.Fatalf("got %d, want 155 (INFO and CLUSTER excluded)", got)
	}
	if parseCommandstats("") != 0 {
		t.Fatal("empty")
	}
}

func TestStatCPUSeconds(t *testing.T) {
	// a command name with spaces and a ')' must not shift the fields
	line := "4242 (redis-server *:7001)) S 1 4242 4242 0 -1 4194560 1234 0 0 0 250 150 0 0 20 0 5 0 100 0 0"
	got, err := statCPUSeconds(line)
	if err != nil || got != 4.0 {
		t.Fatalf("got %v %v, want (250+150)/100 = 4", got, err)
	}
	if _, err := statCPUSeconds("garbage"); err == nil {
		t.Fatal("want an error")
	}
	if _, err := statCPUSeconds("1 (x) S 1 2"); err == nil {
		t.Fatal("too few fields must be an error")
	}
}

func reply(t *testing.T, raw string) resp.Reply {
	t.Helper()
	r, err := resp.NewReader(strings.NewReader(raw)).ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The lost-write check reads these; a failed read is an error, never 0.
func TestCounterAndScoreSum(t *testing.T) {
	if n, err := counter(reply(t, "$4\r\n2000\r\n"), nil); n != 2000 || err != nil {
		t.Fatalf("%d %v", n, err)
	}
	if n, err := counter(reply(t, "$-1\r\n"), nil); n != 0 || err != nil {
		t.Fatalf("missing key: %d %v", n, err)
	}
	if _, err := counter(resp.Reply{}, errors.New("connection lost")); err == nil {
		t.Fatal("a failed read must be an error")
	}
	if _, err := counter(reply(t, "-MOVED 1 x:1\r\n"), nil); err == nil {
		t.Fatal("an error reply must be an error")
	}
	if s, err := scoreSum(reply(t, "*4\r\n$5\r\nuser1\r\n$3\r\n2.5\r\n$5\r\nuser2\r\n$1\r\n7\r\n"), nil); s != 9.5 || err != nil {
		t.Fatalf("%v %v", s, err)
	}
	if s, err := scoreSum(reply(t, "*0\r\n"), nil); s != 0 || err != nil {
		t.Fatalf("empty: %v %v", s, err)
	}
	if _, err := scoreSum(resp.Reply{}, errors.New("timeout")); err == nil {
		t.Fatal("a failed read must be an error")
	}
}
