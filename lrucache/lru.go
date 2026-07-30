package lrucache

import (
	"context"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/incroy/entcache"
	"golang.org/x/sync/singleflight"
)

type (
	// LRU provides a thread-safe LRU cache using hashicorp/golang-lru/v2.
	LRU struct {
		cache *lru.Cache[string, any]
		group singleflight.Group
		mu    sync.Mutex
		waits map[string]chan struct{}
	}
	entry struct {
		*entcache.Entry
		expiry time.Time
	}
)

var (
	_ entcache.Cache          = (*LRU)(nil)
	_ entcache.StampedeLocker = (*LRU)(nil)
)

// New creates a new LRU cache level.
// If maxEntries <= 0, a default capacity of 1000 is used.
func New(maxEntries int) (*LRU, error) {
	if maxEntries <= 0 {
		maxEntries = 1000
	}
	c, err := lru.New[string, any](maxEntries)
	if err != nil {
		return nil, err
	}
	return &LRU{
		cache: c,
		waits: make(map[string]chan struct{}),
	}, nil
}

// MustNew creates a new LRU cache level or panics on error.
func MustNew(maxEntries int) *LRU {
	c, err := New(maxEntries)
	if err != nil {
		panic(err)
	}
	return c
}

// Add adds the entry to the cache.
func (l *LRU) Add(ctx context.Context, k entcache.Key, e *entcache.Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	ne := &entcache.Entry{}
	if err := ne.UnmarshalBinary(buf); err != nil {
		return err
	}
	if ttl == 0 {
		l.cache.Add(key, ne)
	} else {
		l.cache.Add(key, &entry{Entry: ne, expiry: time.Now().Add(ttl)})
	}
	l.mu.Lock()
	if ch, ok := l.waits[key]; ok {
		close(ch)
		delete(l.waits, key)
	}
	l.mu.Unlock()
	return nil
}

// Get gets an entry from the cache.
func (l *LRU) Get(ctx context.Context, k entcache.Key) (*entcache.Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, entcache.ErrNotFound
	}
	val, ok := l.cache.Get(key)
	if !ok {
		return nil, entcache.ErrNotFound
	}
	switch e := val.(type) {
	case *entcache.Entry:
		return e, nil
	case *entry:
		if time.Now().Before(e.expiry) {
			return e.Entry, nil
		}
		l.cache.Remove(key)
		return nil, entcache.ErrNotFound
	default:
		return nil, fmt.Errorf("entcache/lrucache: unexpected entry type: %T", e)
	}
}

// Del deletes an entry from the cache.
func (l *LRU) Del(ctx context.Context, k entcache.Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	l.cache.Remove(key)
	return nil
}

// LockOrWait implements StampedeLocker interface using singleflight and wait channels.
func (l *LRU) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	key := fmt.Sprint(k)
	l.mu.Lock()
	if ch, ok := l.waits[key]; ok {
		l.mu.Unlock()
		wait := func(c context.Context) (*entcache.Entry, error) {
			select {
			case <-c.Done():
				return nil, c.Err()
			case <-ch:
				return l.Get(c, k)
			}
		}
		return false, wait, nil, nil
	}

	ch := make(chan struct{})
	l.waits[key] = ch
	l.mu.Unlock()

	release := func(_ context.Context) {
		l.mu.Lock()
		if ch, ok := l.waits[key]; ok {
			close(ch)
			delete(l.waits, key)
		}
		l.mu.Unlock()
	}
	return true, nil, release, nil
}
