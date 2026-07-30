package rediscache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/incroy/entcache"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

var (
	_ entcache.Cache          = (*Redis)(nil)
	_ entcache.StampedeLocker = (*Redis)(nil)
)

// Redis provides a remote cache backed by go-redis.
type Redis struct {
	c     redis.Cmdable
	group singleflight.Group
	mu    sync.Mutex
	waits map[string]chan struct{}
}

var (
	_ entcache.Cache          = (*Redis)(nil)
	_ entcache.StampedeLocker = (*Redis)(nil)
)

// New returns a new Redis cache level.
func New(c redis.Cmdable) *Redis {
	return &Redis{
		c:     c,
		waits: make(map[string]chan struct{}),
	}
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
	if err := r.c.Set(ctx, key, buf, ttl).Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if ch, ok := r.waits[key]; ok {
		close(ch)
		delete(r.waits, key)
	}
	r.mu.Unlock()
	return nil
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

// Close closes the cache.
func (r *Redis) Close() error {
	return nil
}

// LockOrWait implements StampedeLocker interface for rediscache.
func (r *Redis) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	key := fmt.Sprint(k)
	r.mu.Lock()
	if ch, ok := r.waits[key]; ok {
		r.mu.Unlock()
		wait := func(c context.Context) (*entcache.Entry, error) {
			select {
			case <-c.Done():
				return nil, c.Err()
			case <-ch:
				return r.Get(c, k)
			}
		}
		return false, wait, nil, nil
	}

	ch := make(chan struct{})
	r.waits[key] = ch
	r.mu.Unlock()

	release := func(_ context.Context) {
		r.mu.Lock()
		if ch, ok := r.waits[key]; ok {
			close(ch)
			delete(r.waits, key)
		}
		r.mu.Unlock()
	}
	return true, nil, release, nil
}
