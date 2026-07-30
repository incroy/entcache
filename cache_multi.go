package entcache

import (
	"context"
	"time"
)

// multiLevel provides a multi-level cache implementation.
type multiLevel struct {
	levels []Cache
}

var (
	_ Cache          = (*multiLevel)(nil)
	_ StampedeLocker = (*multiLevel)(nil)
)

// Add adds the entry to the cache.
func (m *multiLevel) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	for i := range m.levels {
		if err := m.levels[i].Add(ctx, k, e, ttl); err != nil {
			return err
		}
	}
	return nil
}

// Get gets an entry from the cache.
func (m *multiLevel) Get(ctx context.Context, k Key) (*Entry, error) {
	for i := range m.levels {
		switch e, err := m.levels[i].Get(ctx, k); {
		case err == nil:
			return e, nil
		case err != ErrNotFound:
			return nil, err
		}
	}
	return nil, ErrNotFound
}

// Del deletes an entry from the cache.
func (m *multiLevel) Del(ctx context.Context, k Key) error {
	for i := range m.levels {
		if err := m.levels[i].Del(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// LockOrWait implements StampedeLocker interface for multiLevel cache.
func (m *multiLevel) LockOrWait(ctx context.Context, k Key) (bool, func(context.Context) (*Entry, error), func(context.Context), error) {
	for i := range m.levels {
		if locker, ok := m.levels[i].(StampedeLocker); ok {
			return locker.LockOrWait(ctx, k)
		}
	}
	return true, nil, nil, nil
}
