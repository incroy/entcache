package entcache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type ctxKey struct{}

type memoryCache struct {
	mu    sync.RWMutex
	items map[string]*Entry
}

func newMemoryCache() *memoryCache {
	return &memoryCache{items: make(map[string]*Entry)}
}

func (m *memoryCache) Get(_ context.Context, k Key) (*Entry, error) {
	key := fmt.Sprint(k)
	m.mu.RLock()
	e, ok := m.items[key]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	return e, nil
}

func (m *memoryCache) Add(_ context.Context, k Key, e *Entry, _ time.Duration) error {
	key := fmt.Sprint(k)
	m.mu.Lock()
	m.items[key] = e
	m.mu.Unlock()
	return nil
}

func (m *memoryCache) Del(_ context.Context, k Key) error {
	key := fmt.Sprint(k)
	m.mu.Lock()
	delete(m.items, key)
	m.mu.Unlock()
	return nil
}

// NewContext returns a new Context that carries a cache.
func NewContext(ctx context.Context, levels ...AddGetDeleter) context.Context {
	var cache AddGetDeleter
	switch len(levels) {
	case 0:
		cache = newMemoryCache()
	case 1:
		cache = levels[0]
	default:
		cache = &multiLevel{levels: levels}
	}
	return context.WithValue(ctx, ctxKey{}, cache)
}

// FromContext returns the cache value stored in ctx, if any.
func FromContext(ctx context.Context) (AddGetDeleter, bool) {
	c, ok := ctx.Value(ctxKey{}).(AddGetDeleter)
	return c, ok
}

// ctxOptions allows injecting runtime options.
type ctxOptions struct {
	skip         bool          // i.e. skip entry.
	evict        bool          // i.e. skip and invalidate entry.
	key          Key           // entry key.
	ttl          time.Duration // entry duration.
	ref          bool          // indicates this is a key-addressed query (e.g. Get by ID).
	skipNotFound bool          // skip cache if query returns 0 rows.
}

var ctxOptionsKey ctxOptions

// Skip returns a new Context that tells the Driver
// to skip the cache entry on Query.
//
//	client.T.Query().All(entcache.Skip(ctx))
func Skip(ctx context.Context) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{skip: true})
	}
	c.skip = true
	return ctx
}

// Evict returns a new Context that tells the Driver
// to skip and invalidate the cache entry on Query.
//
//	client.T.Query().All(entcache.Evict(ctx))
func Evict(ctx context.Context) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{skip: true, evict: true})
	}
	c.skip = true
	c.evict = true
	return ctx
}

// WithKey returns a new Context that carries the Key for the cache entry.
// Note that, this option should not be used if the ent.Client query involves
// more than 1 SQL query (e.g. eager loading).
//
//	client.T.Query().All(entcache.WithKey(ctx, "key"))
func WithKey(ctx context.Context, key Key) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{key: key})
	}
	c.key = key
	return ctx
}

// WithTTL returns a new Context that carries the TTL for the cache entry.
//
//	client.T.Query().All(entcache.WithTTL(ctx, time.Second))
func WithTTL(ctx context.Context, ttl time.Duration) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{ttl: ttl})
	}
	c.ttl = ttl
	return ctx
}

// WithEntryKey returns a new Context with a structured entity key (e.g.
// "User:42") and marks the query as key-addressed. Key-addressed queries
// are eligible for the longer KeyTTL and precise invalidation via ChangeSet.
//
//	client.User.Get(entcache.WithEntryKey(ctx, "User", 42), 42)
func WithEntryKey(ctx context.Context, typ string, id any) context.Context {
	key := fmt.Sprintf("%s:%v", typ, id)
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{key: Key(key), ref: true})
	}
	c.key = Key(key)
	c.ref = true
	return ctx
}

// SkipNotFound returns a new Context that tells the Driver to skip caching
// when the query result contains zero rows. This prevents caching empty
// results for entities that may be created shortly after.
//
//	client.User.Get(entcache.SkipNotFound(ctx), 42)
func SkipNotFound(ctx context.Context) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{skipNotFound: true})
	}
	c.skipNotFound = true
	return ctx
}
