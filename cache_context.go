package entcache

import (
	"context"
	"time"
)

// contextLevel provides a context/request level cache implementation.
type contextLevel struct{}

// Get gets an entry from the cache.
func (*contextLevel) Get(ctx context.Context, k Key) (*Entry, error) {
	c, ok := FromContext(ctx)
	if !ok {
		return nil, ErrNotFound
	}
	return c.Get(ctx, k)
}

// Add adds the entry to the cache.
func (*contextLevel) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	c, ok := FromContext(ctx)
	if !ok {
		return nil
	}
	return c.Add(ctx, k, e, ttl)
}

// Del deletes an entry from the cache.
func (*contextLevel) Del(ctx context.Context, k Key) error {
	c, ok := FromContext(ctx)
	if !ok {
		return nil
	}
	return c.Del(ctx, k)
}
