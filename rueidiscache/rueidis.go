package rueidiscache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/incroy/entcache"
	"github.com/redis/rueidis"
)

// Rueidis provides a remote cache backed by rueidis.
//
// Features activated natively out-of-the-box:
//   - RESP3 Client-Side Caching: Rueidis automatically caches keys in-memory on the client
//     and receives server-assisted invalidation tracking messages from Redis.
//   - Stampede Protection: Channel-based wait locks prevent multiple concurrent database calls.
var (
	_ entcache.Cache          = (*Rueidis)(nil)
	_ entcache.StampedeLocker = (*Rueidis)(nil)
)

// Rueidis is a cache implementation that uses Rueidis.
type Rueidis struct {
	c      rueidis.Client
	mu     sync.Mutex
	id     string
	ctx    context.Context
	cancel context.CancelFunc
}

// New returns a new Rueidis cache level.
func New(c rueidis.Client) *Rueidis {
	ctx, cancel := context.WithCancel(context.Background())
	return &Rueidis{
		c:      c,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Add adds the entry to the cache and unblocks waiting callers.
func (r *Rueidis) Add(ctx context.Context, k entcache.Key, e *entcache.Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	b, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	cmd := r.c.B().Set().Key(key).Value(string(b)).Px(ttl).Build()
	if err := r.c.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	_ = r.c.Do(ctx, r.c.B().Publish().Channel("entcache:invalidations").Message(key).Build()).Error()
	return nil
}

// Get gets the entry from the cache.
func (r *Rueidis) Get(ctx context.Context, k entcache.Key) (*entcache.Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, entcache.ErrNotFound
	}
	cmd := r.c.B().Get().Key(key).Cache()
	resp := r.c.DoCache(ctx, cmd, 0)
	if rueidis.IsRedisNil(resp.Error()) {
		return nil, entcache.ErrNotFound
	}
	b, err := resp.AsBytes()
	if err != nil {
		return nil, err
	}
	var e entcache.Entry
	if err := e.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return &e, nil
}

// Del deletes an entry from the cache.
func (r *Rueidis) Del(ctx context.Context, k entcache.Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	cmd := r.c.B().Del().Key(key).Build()
	if err := r.c.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	_ = r.c.Do(ctx, r.c.B().Publish().Channel("entcache:invalidations").Message(key).Build()).Error()
	return nil
}

// WatchInvalidations watches for cache invalidation events and triggers the provided callback.
func (r *Rueidis) WatchInvalidations(ctx context.Context, onInvalidate func(key entcache.Key)) error {
	go func() {
		cc, cancel := r.c.Dedicate()
		defer cancel()
		_ = cc.Receive(ctx, r.c.B().Subscribe().Channel("entcache:invalidations").Build(), func(msg rueidis.PubSubMessage) {
			if msg.Channel == "entcache:invalidations" {
				onInvalidate(msg.Message)
			}
		})
	}()
	return nil
}

// Close closes the cache.
func (r *Rueidis) Close() error {
	r.cancel()
	r.mu.Lock()
	id := r.id
	r.mu.Unlock()
	if id != "" {
		r.c.Do(context.Background(), r.c.B().Del().Key(id).Build())
	}
	return nil
}

var (
	delkey      = rueidis.NewLuaScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)
	acquireLock = rueidis.NewLuaScript(`if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then return nil else return redis.call("GET", KEYS[1]) end`)
)

func (r *Rueidis) refresh(id string) {
	for {
		select {
		case <-time.After(5 * time.Second):
			r.mu.Lock()
			id2 := r.id
			r.mu.Unlock()
			if id2 != id {
				return // client id has changed, abort
			}
			r.c.Do(r.ctx, r.c.B().Set().Key(id).Value("").Px(10*time.Second).Build())
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Rueidis) keepalive() (string, error) {
	r.mu.Lock()
	id := r.id
	r.mu.Unlock()
	if id == "" {
		id = fmt.Sprintf("rueidisid:%d", time.Now().UnixNano())
		if err := r.c.Do(r.ctx, r.c.B().Set().Key(id).Value("").Px(10*time.Second).Build()).Error(); err == nil {
			r.mu.Lock()
			if r.id == "" {
				r.id = id
				go r.refresh(id)
			} else {
				id = r.id
			}
			r.mu.Unlock()
		} else {
			return "", err
		}
	}
	return id, nil
}

// LockOrWait implements StampedeLocker interface using RESP3 Client-Tracking.
func (r *Rueidis) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	key := fmt.Sprint(k)
	lockKey := "lock:" + key
	id, err := r.keepalive()
	if err != nil {
		return false, nil, nil, err
	}
	ttlMs := "30000" // 30 seconds

	// Acquire lock
	resp := acquireLock.Exec(ctx, r.c, []string{lockKey}, []string{id, ttlMs})
	val, err := resp.ToString()

	// If err is RedisNil, it means the script returned `nil`, which means we WON the lock.
	if rueidis.IsRedisNil(err) {
		// Winner (Loader)
		release := func(c context.Context) {
			_ = delkey.Exec(c, r.c, []string{lockKey}, []string{id}).Error()
		}
		return true, nil, release, nil
	}

	if err != nil {
		return false, nil, nil, err
	}

	// Waiter
	wait := func(c context.Context) (*entcache.Entry, error) {
		// Use RESP3 client-side caching (DoCache) to wait for the lock ID to be deleted.
		// `val` contains the current holder's heartbeat ID.
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.Done():
				return nil, c.Err()
			case <-ticker.C:
				err := r.c.DoCache(c, r.c.B().Get().Key(lockKey).Cache(), 30*time.Second).Error()
				if rueidis.IsRedisNil(err) {
					// Lock was deleted, see if data was written
					resp := r.c.DoCache(c, r.c.B().Get().Key(key).Cache(), 0)
					if !rueidis.IsRedisNil(resp.Error()) {
						if b, err := resp.AsBytes(); err == nil {
							var e entcache.Entry
							if err := e.UnmarshalBinary(b); err == nil {
								return &e, nil
							}
						}
					}
					return nil, entcache.ErrRetryLocker
				}
				// If another winner acquired the lock, the ID will change, but for our simple Wait API,
				// we just retry so the outer retry loop will call LockOrWait again.
				val2, err2 := r.c.DoCache(c, r.c.B().Get().Key(lockKey).Cache(), 30*time.Second).ToString()
				if err2 == nil && val2 != val {
					// Lock changed hands
					return nil, entcache.ErrRetryLocker
				}
				
				// Check if the current holder crashed
				if val != "" && strings.HasPrefix(val, "rueidisid:") {
					err3 := r.c.DoCache(c, r.c.B().Get().Key(val).Cache(), 10*time.Second).Error()
					if rueidis.IsRedisNil(err3) {
						// The client who held the lock crashed
						delkey.Exec(context.Background(), r.c, []string{lockKey}, []string{val})
						return nil, entcache.ErrRetryLocker
					}
				}
				
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return false, wait, nil, nil
}
