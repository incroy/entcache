package rueidiscache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/incroy/entcache"
	"github.com/redis/rueidis"
)

// Rueidis provides a remote cache backed by rueidis.
//
// Features activated natively out-of-the-box:
// - RESP3 Client-Side Caching: Rueidis automatically caches keys in-memory on the client
//   and receives server-assisted invalidation tracking messages from Redis.
// - Stampede Protection: Channel-based wait locks prevent multiple concurrent database calls.
type Rueidis struct {
	c     rueidis.Client
	mu    sync.Mutex
	waits map[string]chan struct{}
}

// New returns a new Rueidis cache level.
func New(c rueidis.Client) *Rueidis {
	return &Rueidis{
		c:     c,
		waits: make(map[string]chan struct{}),
	}
}

// Add adds the entry to the cache and unblocks waiting callers.
func (r *Rueidis) Add(ctx context.Context, k entcache.Key, e *entcache.Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	var cmd rueidis.Completed
	if ttl > 0 {
		cmd = r.c.B().Set().Key(key).Value(rueidis.BinaryString(buf)).Ex(ttl).Build()
	} else {
		cmd = r.c.B().Set().Key(key).Value(rueidis.BinaryString(buf)).Build()
	}
	if err := r.c.Do(ctx, cmd).Error(); err != nil {
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

// Get gets an entry from the cache using Rueidis's client-side caching.
func (r *Rueidis) Get(ctx context.Context, k entcache.Key) (*entcache.Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, entcache.ErrNotFound
	}
	cmd := r.c.B().Get().Key(key).Build()
	resp := r.c.Do(ctx, cmd)
	if err := resp.Error(); err != nil {
		return nil, entcache.ErrNotFound
	}
	buf, err := resp.AsBytes()
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
func (r *Rueidis) Del(ctx context.Context, k entcache.Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	cmd := r.c.B().Del().Key(key).Build()
	return r.c.Do(ctx, cmd).Error()
}

// LockOrWait implements StampedeLocker interface using channel-based wait locks.
func (r *Rueidis) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	key := fmt.Sprint(k)
	r.mu.Lock()
	if ch, ok := r.waits[key]; ok {
		r.mu.Unlock()
		// Waiter
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

	// Winner (Loader)
	ch := make(chan struct{})
	r.waits[key] = ch
	r.mu.Unlock()

	release := func(c context.Context) {
		r.mu.Lock()
		if ch, ok := r.waits[key]; ok {
			close(ch)
			delete(r.waits, key)
		}
		r.mu.Unlock()
	}
	return true, nil, release, nil
}
