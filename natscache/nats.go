package natscache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/incroy/entcache"
	"github.com/nats-io/nats.go/jetstream"
)

const placeholderSuffix = "_lock"

// NatsKV provides a remote cache backed by NATS JetStream KeyValue
// with built-in distributed stampede locking and real-time Watch invalidations.
type NatsKV struct {
	kv jetstream.KeyValue
}

var (
	_ entcache.Cache          = (*NatsKV)(nil)
	_ entcache.StampedeLocker = (*NatsKV)(nil)
	_ entcache.Invalidator    = (*NatsKV)(nil)
)

// New returns a new NATS JetStream KeyValue cache level.
func New(kv jetstream.KeyValue) *NatsKV {
	return &NatsKV{kv: kv}
}

// Add adds the entry to the cache using Put or Create for per-key TTL.
func (n *NatsKV) Add(ctx context.Context, k entcache.Key, e *entcache.Entry, ttl time.Duration) error {
	key := sanitizeKey(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	if ttl > 0 {
		_ = n.kv.Delete(ctx, key)
		_, err = n.kv.Create(ctx, key, buf, jetstream.KeyTTL(ttl))
	} else {
		_, err = n.kv.Put(ctx, key, buf)
	}
	return err
}

// Get gets an entry from the cache.
func (n *NatsKV) Get(ctx context.Context, k entcache.Key) (*entcache.Entry, error) {
	key := sanitizeKey(k)
	if key == "" {
		return nil, entcache.ErrNotFound
	}
	kve, err := n.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, entcache.ErrNotFound
		}
		return nil, err
	}
	buf := kve.Value()
	if len(buf) == 0 {
		return nil, entcache.ErrNotFound
	}
	e := &entcache.Entry{}
	if err := e.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return e, nil
}

// Del deletes an entry from the cache.
func (n *NatsKV) Del(ctx context.Context, k entcache.Key) error {
	key := sanitizeKey(k)
	if key == "" {
		return nil
	}
	err := n.kv.Delete(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}

// LockOrWait acquires a distributed lock on cache misses using NATS KV Create.
// If won == true, the caller executes the DB query and releases the lock.
// If won == false, wait() uses Watch to wait until the winner populates the cache.
func (n *NatsKV) LockOrWait(ctx context.Context, k entcache.Key) (bool, func(context.Context) (*entcache.Entry, error), func(context.Context), error) {
	key := sanitizeKey(k)
	lockKey := key + placeholderSuffix

	// Attempt atomic lock creation
	_, err := n.kv.Create(ctx, lockKey, []byte("1"), jetstream.KeyTTL(10*time.Second))
	if err == nil {
		// Winner (Loader)
		release := func(c context.Context) {
			_ = n.kv.Delete(c, lockKey)
		}
		return true, nil, release, nil
	}

	// Loser (Waiter): Someone else is loading — watch the key or lockKey until value is written
	wait := func(c context.Context) (*entcache.Entry, error) {
		watcher, wErr := n.kv.Watch(c, key)
		if wErr != nil {
			return nil, wErr
		}
		defer watcher.Stop()

		var once sync.Once
		done := make(chan struct{})
		defer once.Do(func() { close(done) })

		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()

		for {
			select {
			case <-c.Done():
				return nil, c.Err()
			case <-timer.C:
				return nil, entcache.ErrNotFound
			case update, ok := <-watcher.Updates():
				if !ok {
					return nil, entcache.ErrNotFound
				}
				if update != nil && len(update.Value()) > 0 {
					e := &entcache.Entry{}
					if err := e.UnmarshalBinary(update.Value()); err == nil {
						return e, nil
					}
				}
			}
		}
	}

	return false, wait, nil, nil
}

// WatchInvalidations listens to NATS KV key updates/deletions and triggers onInvalidate.
func (n *NatsKV) WatchInvalidations(ctx context.Context, onInvalidate func(key entcache.Key)) error {
	watcher, err := n.kv.Watch(ctx, ">")
	if err != nil {
		return err
	}
	go func() {
		defer watcher.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case update, ok := <-watcher.Updates():
				if !ok {
					return
				}
				if update != nil {
					op := update.Operation()
					if op == jetstream.KeyValueDelete || op == jetstream.KeyValuePurge {
						k := update.Key()
						if !strings.HasSuffix(k, placeholderSuffix) {
							onInvalidate(k)
						}
					}
				}
			}
		}
	}()
	return nil
}

func sanitizeKey(k entcache.Key) string {
	s := fmt.Sprint(k)
	return strings.ReplaceAll(s, ":", "_")
}
