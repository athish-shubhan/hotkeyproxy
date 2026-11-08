// Package cluster maps keys to Redis Cluster hash slots and slots to shards.
package cluster

import (
	"bytes"
	"fmt"
	"net"
	"strconv"

	"hotkeyproxy/resp"
)

const Slots = 16384

var crcTable [256]uint16

func init() {
	for i := 0; i < 256; i++ {
		crc := uint16(i) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		crcTable[i] = crc
	}
}

// CRC16 is CRC-16/XMODEM, the checksum Redis Cluster uses.
func CRC16(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^c]
	}
	return crc
}

// Slot implements Redis Cluster key hashing, including {hash tags}.
func Slot(key []byte) int {
	if s := bytes.IndexByte(key, '{'); s >= 0 {
		if e := bytes.IndexByte(key[s+1:], '}'); e > 0 {
			key = key[s+1 : s+1+e]
		}
	}
	return int(CRC16(key)) % Slots
}

// Map assigns every slot to a shard index; Addrs[i] is shard i's address.
type Map struct {
	Owner [Slots]int
	Addrs []string
}

func (m *Map) Shard(key []byte) int { return m.Owner[Slot(key)] }

// FromClusterSlots builds the map from a CLUSTER SLOTS reply. Each entry is
// [start, end, [host, port, ...] (the master), replicas...]. An empty host
// means the node does not know its own address; Redis clients then use the
// address they queried, which here is 127.0.0.1 (the local benchmark
// cluster). A range outside 0..16383 or a slot left uncovered is an error.
func FromClusterSlots(rep resp.Reply) (*Map, error) {
	if rep.Type != '*' {
		return nil, fmt.Errorf("CLUSTER SLOTS: unexpected reply %q", rep.Raw)
	}
	m := &Map{}
	for i := range m.Owner {
		m.Owner[i] = -1
	}
	idx := map[string]int{}
	for _, r := range rep.Elems {
		if len(r.Elems) < 3 {
			continue
		}
		lo, hi := r.Elems[0].Int, r.Elems[1].Int
		if lo < 0 || hi >= Slots || lo > hi {
			// used to index Owner out of range (panic) for a negative start
			return nil, fmt.Errorf("CLUSTER SLOTS: bad slot range %d-%d", lo, hi)
		}
		master := r.Elems[2]
		if len(master.Elems) < 2 {
			continue
		}
		host, port := string(master.Elems[0].Str), master.Elems[1].Int
		if port <= 0 || port > 65535 {
			return nil, fmt.Errorf("CLUSTER SLOTS: bad port %d for slots %d-%d", port, lo, hi)
		}
		if host == "" {
			host = "127.0.0.1"
		}
		// JoinHostPort brackets IPv6 hosts ("[::1]:7000"); plain
		// concatenation gave an address net.Dial cannot parse.
		addr := net.JoinHostPort(host, strconv.FormatInt(port, 10))
		id, ok := idx[addr]
		if !ok {
			id = len(m.Addrs)
			idx[addr] = id
			m.Addrs = append(m.Addrs, addr)
		}
		for s := lo; s <= hi; s++ {
			m.Owner[s] = id
		}
	}
	for s, o := range m.Owner {
		if o < 0 {
			return nil, fmt.Errorf("slot %d not covered", s)
		}
	}
	return m, nil
}
