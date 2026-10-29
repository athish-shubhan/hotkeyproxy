// Package cluster maps keys to Redis Cluster hash slots and slots to shards.
package cluster

import (
	"bytes"
	"fmt"
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

// FromClusterSlots builds the map from a CLUSTER SLOTS reply.
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
		lo, hi := int(r.Elems[0].Int), int(r.Elems[1].Int)
		master := r.Elems[2]
		if len(master.Elems) < 2 {
			continue
		}
		addr := string(master.Elems[0].Str) + ":" + strconv.FormatInt(master.Elems[1].Int, 10)
		if string(master.Elems[0].Str) == "" {
			addr = "127.0.0.1:" + strconv.FormatInt(master.Elems[1].Int, 10)
		}
		id, ok := idx[addr]
		if !ok {
			id = len(m.Addrs)
			idx[addr] = id
			m.Addrs = append(m.Addrs, addr)
		}
		for s := lo; s <= hi && s < Slots; s++ {
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
