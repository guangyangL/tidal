package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrDuplicate = errors.New("duplicate request")

type Deduplicator struct {
	rdb *redis.Client
}

func NewDeduplicator(rdb *redis.Client) *Deduplicator {
	return &Deduplicator{rdb: rdb}
}

// SETNX sets the key with 5s TTL. Returns ErrDuplicate if the key already exists.
func (d *Deduplicator) SETNX(ctx context.Context, key string) error {
	ok, err := d.rdb.SetNX(ctx, key, "1", 600*time.Second).Result()
	if err != nil {
		return err
	}
	if !ok {
		return ErrDuplicate
	}
	return nil
}
