package idgen

import (
	"fmt"
	"math/big"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// Sonyflake epoch: 2026-01-01 00:00:00 UTC in milliseconds
const epoch int64 = 1767225600000

const (
	workerBits uint8 = 16
	seqBits    uint8 = 8
)

type Sonyflake struct {
	mu      sync.Mutex
	worker  uint16
	seq     uint8
	elapsed int64
}

func New() (*Sonyflake, error) {
	worker, err := lower16BitPrivateIP()
	if err != nil {
		worker = uint16(hash(os.Getenv("HOSTNAME"))) & 0xFFFF
	}
	return &Sonyflake{worker: worker}, nil
}

func (s *Sonyflake) Next() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli() - epoch
	if now == s.elapsed {
		s.seq++
		if s.seq == 0 {
			// sequence overflow, wait for next millisecond
			for now <= s.elapsed {
				now = time.Now().UnixMilli() - epoch
			}
		}
	} else {
		s.seq = 0
	}
	s.elapsed = now

	return now<<(workerBits+seqBits) | int64(s.worker)<<seqBits | int64(s.seq), nil
}

func lower16BitPrivateIP() (uint16, error) {
	ip, err := getPrivateIP()
	if err != nil {
		return 0, err
	}
	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

func getPrivateIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip := ipnet.IP.To4(); ip != nil {
				return ip, nil
			}
		}
	}
	return nil, fmt.Errorf("no private IP address found")
}

func hash(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*31 + uint32(s[i])
	}
	return h
}

func EncodeBase62(src []byte) string {
	const chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	n := new(big.Int).SetBytes(src)
	if n.Sign() == 0 {
		return string(chars[0])
	}
	div := big.NewInt(62)
	mod := new(big.Int)
	var dst [22]byte
	i := 22
	for n.Sign() > 0 {
		i--
		n.DivMod(n, div, mod)
		dst[i] = chars[mod.Int64()]
	}
	return string(dst[i:])
}

func init() {
	// suppress unused import warning
	_ = strconv.Itoa
}
