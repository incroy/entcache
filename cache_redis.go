package entcache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis provides a remote cache backed by go-redis
// and implements the AddGetDeleter interface.
type Redis struct {
	c redis.Cmdable
}

// NewRedis returns a new Redis cache level from the given Redis connection.
//
//	entcache.NewRedis(redis.NewClient(&redis.Options{
//		Addr: ":6379"
//	}))
//
//	entcache.NewRedis(redis.NewClusterClient(&redis.ClusterOptions{
//		Addrs: []string{":7000", ":7001", ":7002"},
//	}))
func NewRedis(c redis.Cmdable) *Redis {
	return &Redis{c: c}
}

// Add adds the entry to the cache.
func (r *Redis) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	if err := r.c.Set(ctx, key, buf, ttl).Err(); err != nil {
		return err
	}
	return nil
}

// Get gets an entry from the cache.
func (r *Redis) Get(ctx context.Context, k Key) (*Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, ErrNotFound
	}
	buf, err := r.c.Get(ctx, key).Bytes()
	if err != nil || len(buf) == 0 {
		return nil, ErrNotFound
	}
	e := &Entry{}
	if err := e.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return e, nil
}

// Del deletes an entry from the cache.
func (r *Redis) Del(ctx context.Context, k Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	return r.c.Del(ctx, key).Err()
}
