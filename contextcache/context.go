package contextcache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/incroy/entcache"
)

type ctxKey struct{}

type memoryEntry struct {
	*entcache.Entry
	expiry time.Time
}

type memoryCache struct {
	mu    sync.RWMutex
	items map[string]*memoryEntry
	waits map[string]chan struct{}
}

// Context provides a request/context-level cache.
type Context struct{}

var (
	_ entcache.Cache          = (*Context)(nil)
	_ entcache.StampedeLocker = (*Context)(nil)
)

// New returns a new context-level cache level.
func New() *Context {
	return &Context{}
}

// NewContext attaches a memory cache to the provided context.
func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, &memoryCache{
		items: make(map[string]*memoryEntry),
		waits: make(map[string]chan struct{}),
	})
}

func fromContext(ctx context.Context) (*memoryCache, bool) {
	m, ok := ctx.Value(ctxKey{}).(*memoryCache)
	return m, ok
}

// Get gets an entry from the context-level cache.
func (c *Context) Get(ctx context.Context, k entcache.Key) (*entcache.Entry, error) {
	m, ok := fromContext(ctx)
	if !ok {
		return nil, entcache.ErrNotFound
	}
	return m.Get(ctx, k)
}

// Add adds an entry to the context-level cache.
func (c *Context) Add(ctx context.Context, k entcache.Key, e *entcache.Entry, ttl time.Duration) error {
	m, ok := fromContext(ctx)
	if !ok {
		return nil
	}
	return m.Add(ctx, k, e, ttl)
}

// Del deletes an entry from the context-level cache.
func (c *Context) Del(ctx context.Context, k entcache.Key) error {
	m, ok := fromContext(ctx)
	if !ok {
		return nil
	}
	return m.Del(ctx, k)
}

// LockOrWait implements StampedeLocker interface for context-level cache.
func (c *Context) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	m, ok := fromContext(ctx)
	if !ok {
		return true, nil, nil, nil
	}
	return m.LockOrWait(ctx, k)
}

func (m *memoryCache) Get(_ context.Context, k entcache.Key) (*entcache.Entry, error) {
	key := fmt.Sprint(k)
	m.mu.RLock()
	me, ok := m.items[key]
	m.mu.RUnlock()
	if !ok {
		return nil, entcache.ErrNotFound
	}
	if !me.expiry.IsZero() && time.Now().After(me.expiry) {
		_ = m.Del(context.Background(), k)
		return nil, entcache.ErrNotFound
	}
	return me.Entry, nil
}

func (m *memoryCache) Add(_ context.Context, k entcache.Key, e *entcache.Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	m.mu.Lock()
	if ttl < 0 {
		delete(m.items, key)
	} else if ttl > 0 {
		m.items[key] = &memoryEntry{Entry: e, expiry: time.Now().Add(ttl)}
	} else {
		m.items[key] = &memoryEntry{Entry: e}
	}
	if ch, ok := m.waits[key]; ok {
		close(ch)
		delete(m.waits, key)
	}
	m.mu.Unlock()
	return nil
}

func (m *memoryCache) Del(_ context.Context, k entcache.Key) error {
	key := fmt.Sprint(k)
	m.mu.Lock()
	delete(m.items, key)
	m.mu.Unlock()
	return nil
}

func (m *memoryCache) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	key := fmt.Sprint(k)
	m.mu.Lock()
	if ch, ok := m.waits[key]; ok {
		m.mu.Unlock()
		wait := func(c context.Context) (*entcache.Entry, error) {
			select {
			case <-c.Done():
				return nil, c.Err()
			case <-ch:
				return m.Get(c, k)
			}
		}
		return false, wait, nil, nil
	}

	ch := make(chan struct{})
	m.waits[key] = ch
	m.mu.Unlock()

	release := func(_ context.Context) {
		m.mu.Lock()
		if existing, ok := m.waits[key]; ok && existing == ch {
			close(ch)
			delete(m.waits, key)
		}
		m.mu.Unlock()
	}
	return true, nil, release, nil
}
