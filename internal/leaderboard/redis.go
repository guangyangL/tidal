package leaderboard

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// UpdateScore shifts a user from oldScore to newScore in the segment tree.
// Called by the MQ consumer.
func UpdateScore(ctx context.Context, rdb *redis.Client, tree *SegmentNode, roomID string, oldScore, newScore int) error {
	segKey := segTreeKey(roomID)
	reduces := getSegmentsOnPath(tree, oldScore)
	adds := getSegmentsOnPath(tree, newScore)

	script := `
local M = tonumber(ARGV[1])
local N = tonumber(ARGV[2])
for i=1,M,1 do
    redis.call("HINCRBY", KEYS[1], KEYS[1+i], -1)
end
for i=1,N,1 do
    redis.call("HINCRBY", KEYS[1], KEYS[1+M+i], 1)
end
return 0
`
	keys := make([]string, 0, 1+len(reduces)+len(adds))
	keys = append(keys, segKey)
	keys = append(keys, reduces...)
	keys = append(keys, adds...)

	return rdb.Eval(ctx, script, keys, len(reduces), len(adds)).Err()
}

// AddScore increments the segment tree for a new user (no old score).
func AddScore(ctx context.Context, rdb *redis.Client, tree *SegmentNode, roomID string, score int) error {
	segKey := segTreeKey(roomID)
	segs := getSegmentsOnPath(tree, score)

	script := `
local N = tonumber(ARGV[1])
for i=1,N,1 do
    redis.call("HINCRBY", KEYS[1], KEYS[1+i], 1)
end
return 0
`
	keys := make([]string, 0, 1+len(segs))
	keys = append(keys, segKey)
	keys = append(keys, segs...)

	return rdb.Eval(ctx, script, keys, len(segs)).Err()
}

// GetRank estimates the user's rank from the segment tree.
// Returns the number of users with a HIGHER score.
func GetRank(ctx context.Context, rdb *redis.Client, tree *SegmentNode, roomID string, score int) (int, error) {
	leaf := findSegment(tree, score)
	if leaf == nil {
		return 0, fmt.Errorf("score %d out of range", score)
	}
	segKey := segTreeKey(roomID)
	path := getSegmentsOnPath(tree, score)

	result, err := rdb.HMGet(ctx, segKey, path...).Result()
	if err != nil {
		return 0, err
	}

	leafKey := nodeKey(leaf.lower, leaf.upper)
	var segCounter, biggerCounter int

	for i, val := range result {
		if val == nil {
			continue
		}
		s, ok := val.(string)
		if !ok {
			return 0, fmt.Errorf("unexpected value type: %T", val)
		}
		cnt, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		if path[i] == leafKey {
			segCounter = cnt
		} else {
			biggerCounter += cnt
		}
	}

	bucketSize := leaf.upper - leaf.lower
	if bucketSize <= 0 {
		return biggerCounter, nil
	}
	// linear interpolation within the leaf bucket
	more := float64((leaf.upper-score)*segCounter) / float64(bucketSize)
	return int(more) + biggerCounter, nil
}

func segTreeKey(roomID string) string {
	return fmt.Sprintf("seg_tree:%s", roomID)
}
