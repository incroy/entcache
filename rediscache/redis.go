package rediscache

import (
	"context"
	"fmt"
	"time"

	"github.com/incroy/entcache"
	"github.com/redis/go-redis/v9"
)

// Redis provides a remote cache backed by go-redis.
type Redis struct {
	c redis.Cmdable
}

// New returns a new Redis cache level.
func New(c redis.Cmdable) *Redis {
	return &Redis{c: c}
}

// Add adds the entry to the cache.
func (r *Redis) Add(ctx context.Context, k entcache.Key, e *entcache.Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	return r.c.Set(ctx, key, buf, ttl).Err()
}

// Get gets an entry from the cache.
func (r *Redis) Get(ctx context.Context, k entcache.Key) (*entcache.Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, entcache.ErrNotFound
	}
	buf, err := r.c.Get(ctx, key).Bytes()
	if err != nil || len(buf) == 0 {
		return nil, entcache.ErrNotFound
	}
	e := &entcache.Entry{}
	if err := e.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return e, nil
}

// Del deletes an entry from the cache.
func (r *Redis) Del(ctx context.Context, k entcache.Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	return r.c.Del(ctx, key).Err()
}
