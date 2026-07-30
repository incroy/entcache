package rediscache

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/incroy/entcache"
	"github.com/redis/go-redis/v9"
)

var (
	_ entcache.Cache          = (*Redis)(nil)
	_ entcache.StampedeLocker = (*Redis)(nil)
)

// Option configures the Redis cache instance.
type Option func(*Redis)

// WithKeyspaceEvents enables using Redis Keyspace events (Pub/Sub) to wait for lock deletions,
// instead of network polling. The Redis server must have `notify-keyspace-events KEA` configured.
func WithKeyspaceEvents() Option {
	return func(r *Redis) {
		r.keyspaceEvents = true
	}
}

// Redis is a cache implementation that uses go-redis.
type Redis struct {
	c              redis.Cmdable
	mu             sync.Mutex
	id             string
	ctx            context.Context
	cancel         context.CancelFunc
	keyspaceEvents bool
}

// New returns a new Redis cache level.
func New(c redis.Cmdable, opts ...Option) *Redis {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Redis{
		c:      c,
		ctx:    ctx,
		cancel: cancel,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Add adds the entry to the cache and unblocks waiting callers.
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
	r.c.Publish(ctx, "entcache:invalidations", key)
	return nil
}

// Get gets the entry from the cache.
func (r *Redis) Get(ctx context.Context, k entcache.Key) (*entcache.Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, entcache.ErrNotFound
	}
	buf, err := r.c.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, entcache.ErrNotFound
		}
		return nil, err
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
	if err := r.c.Del(ctx, key).Err(); err != nil {
		return err
	}
	r.c.Publish(ctx, "entcache:invalidations", key)
	return nil
}

// WatchInvalidations watches for cache invalidation events and triggers the provided callback.
func (r *Redis) WatchInvalidations(ctx context.Context, onInvalidate func(key entcache.Key)) error {
	client, ok := r.c.(interface {
		Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	})
	if !ok {
		return fmt.Errorf("underlying redis client does not support Subscribe")
	}

	pubsub := client.Subscribe(ctx, "entcache:invalidations")
	go func() {
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				onInvalidate(msg.Payload)
			}
		}
	}()
	return nil
}

// Close closes the cache.
func (r *Redis) Close() error {
	r.cancel()
	r.mu.Lock()
	id := r.id
	r.mu.Unlock()
	if id != "" {
		r.c.Del(context.Background(), id)
	}
	return nil
}

var (
	delkey      = redis.NewScript(`if redis.call("GET",KEYS[1]) == ARGV[1] then return redis.call("DEL",KEYS[1]) else return 0 end`)
	acquireLock = redis.NewScript(`if redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then return nil else return redis.call("GET", KEYS[1]) end`)
)

func (r *Redis) refresh(id string) {
	for {
		select {
		case <-time.After(5 * time.Second):
			r.mu.Lock()
			id2 := r.id
			r.mu.Unlock()
			if id2 != id {
				return // client id has changed, abort
			}
			r.c.Set(r.ctx, id, "", 10*time.Second)
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *Redis) keepalive() (string, error) {
	r.mu.Lock()
	id := r.id
	r.mu.Unlock()
	if id == "" {
		id = fmt.Sprintf("redisid:%d", time.Now().UnixNano())
		if err := r.c.Set(r.ctx, id, "", 10*time.Second).Err(); err == nil {
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

// LockOrWait implements StampedeLocker interface.
func (r *Redis) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	key := fmt.Sprint(k)
	lockKey := "lock:" + key
	id, err := r.keepalive()
	if err != nil {
		return false, nil, nil, err
	}
	ttlMs := "30000" // 30 seconds

	// Acquire lock
	val, err := acquireLock.Run(ctx, r.c, []string{lockKey}, id, ttlMs).Result()

	// If err is RedisNil, it means the script returned `nil`, which means we WON the lock.
	if err == redis.Nil {
		// Winner (Loader)
		release := func(c context.Context) {
			delkey.Run(c, r.c, []string{lockKey}, id)
		}
		return true, nil, release, nil
	}

	if err != nil {
		return false, nil, nil, err
	}

	valStr, ok := val.(string)
	if !ok {
		return false, nil, nil, fmt.Errorf("unexpected script return type")
	}

	// Waiter
	wait := func(c context.Context) (*entcache.Entry, error) {
		if r.keyspaceEvents {
			return r.waitKeyspace(c, key, lockKey, valStr)
		}
		return r.waitPoll(c, key, lockKey, valStr)
	}
	return false, wait, nil, nil
}

func (r *Redis) waitKeyspace(c context.Context, key, lockKey, valStr string) (*entcache.Entry, error) {
	client, ok := r.c.(interface {
		PSubscribe(ctx context.Context, channels ...string) *redis.PubSub
	})
	if !ok {
		// Fallback to poll if cmdable doesn't support PSubscribe
		return r.waitPoll(c, key, lockKey, valStr)
	}

	pubsub := client.PSubscribe(c, "__keyspace@*__:"+lockKey)
	defer pubsub.Close()

	// Check if the lock is already gone to avoid race condition before subscription is active
	err := r.c.Get(c, lockKey).Err()
	if err == redis.Nil {
		buf, err2 := r.c.Get(c, key).Bytes()
		if err2 == nil {
			var e entcache.Entry
			if err3 := e.UnmarshalBinary(buf); err3 == nil {
				return &e, nil
			}
		}
		return nil, entcache.ErrRetryLocker
	}

	ch := pubsub.Channel()
	for {
		select {
		case <-c.Done():
			return nil, c.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil, entcache.ErrRetryLocker
			}
			if msg.Payload == "del" || msg.Payload == "expired" {
				buf, err2 := r.c.Get(c, key).Bytes()
				if err2 == nil {
					var e entcache.Entry
					if err3 := e.UnmarshalBinary(buf); err3 == nil {
						return &e, nil
					}
				}
				return nil, entcache.ErrRetryLocker
			}
			if msg.Payload == "set" {
				// Lock changed hands
				return nil, entcache.ErrRetryLocker
			}
		}
	}
}

func (r *Redis) waitPoll(c context.Context, key, lockKey, valStr string) (*entcache.Entry, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.Done():
			return nil, c.Err()
		case <-ticker.C:
			err := r.c.Get(c, lockKey).Err()
			if err == redis.Nil {
				// Lock was deleted, see if data was written
				buf, err2 := r.c.Get(c, key).Bytes()
				if err2 == nil {
					var e entcache.Entry
					if err3 := e.UnmarshalBinary(buf); err3 == nil {
						return &e, nil
					}
				}
				return nil, entcache.ErrRetryLocker
			}
			
			val2, err2 := r.c.Get(c, lockKey).Result()
			if err2 == nil && val2 != valStr {
				// Lock changed hands
				return nil, entcache.ErrRetryLocker
			}
			
			// Check if the current holder crashed
			if valStr != "" && strings.HasPrefix(valStr, "redisid:") {
				err3 := r.c.Get(c, valStr).Err()
				if err3 == redis.Nil {
					// The client who held the lock crashed
					delkey.Run(context.Background(), r.c, []string{lockKey}, valStr)
					return nil, entcache.ErrRetryLocker
				}
			}
			
			if err != nil && err != redis.Nil {
				return nil, err
			}
		}
	}
}
