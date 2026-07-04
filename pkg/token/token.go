package token

import (
	"encoding/binary"

	"github.com/guangyang/tidal/pkg/idgen"
)

// Encode generates a trend-ascending batch token:
//
//	user_id(42bit) | anchor_id(42bit) | window_start_ms(42bit)
//	→ 126 bit → 21 char base62
//
// UUID would scatter writes across InnoDB pages; this stays sequential.
func Encode(userID, anchorID int64, windowStartMs int64) string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(userID<<22)|uint64(anchorID>>20))
	binary.BigEndian.PutUint64(buf[8:16], uint64(anchorID<<42)|uint64(windowStartMs))
	return idgen.EncodeBase62(buf[:])
}
