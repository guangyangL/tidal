package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

// Generates vegeta JSON-format targets for load testing Tidal.
// Usage: go run scripts/gen_targets.go [N] > targets.json
// Default N = 100000

type Target struct {
	Method string              `json:"method"`
	URL    string              `json:"url"`
	Header map[string][]string `json:"header"`
	Body   string              `json:"body"`
}

func main() {
	n := 100000
	if len(os.Args) > 1 {
		v, _ := strconv.Atoi(os.Args[1])
		if v > 0 {
			n = v
		}
	}

	seed := make([]byte, 8)
	rand.Read(seed)
	enc := json.NewEncoder(os.Stdout)

	for i := 0; i < n; i++ {
		uid := 10000 + int(fastRand(&seed)%1000)
		room := 1 + int(fastRand(&seed)%10)
		anchor := 1 + int(fastRand(&seed)%100)
		giftID := 1 + int(fastRand(&seed)%5)
		rid := fmt.Sprintf("%d-%x-%d", uid, seed, i)
		body := fmt.Sprintf(`{"room_id":%d,"anchor_id":%d,"gift_id":%d}`, room, anchor, giftID)

		t := Target{
			Method: "POST",
			URL:    "http://localhost:8080/api/v1/gift/send",
			Header: map[string][]string{
				"X-User-ID":    {fmt.Sprintf("%d", uid)},
				"Content-Type": {"application/json"},
				"X-Request-ID": {rid},
			},
			Body: base64.StdEncoding.EncodeToString([]byte(body)),
		}
		enc.Encode(t)
	}
}

func fastRand(seed *[]byte) uint64 {
	s := *seed
	n := binary.LittleEndian.Uint64(s)
	n ^= n << 13
	n ^= n >> 7
	n ^= n << 17
	binary.LittleEndian.PutUint64(s, n)
	return n
}
