package natscache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/incroy/entcache"
	"github.com/nats-io/nats.go/jetstream"
)

// NatsKV provides a remote cache backed by NATS JetStream KeyValue
// with built-in distributed stampede locking and real-time Watch invalidations.
type NatsKV struct {
	kv     jetstream.KeyValue
	mu     sync.Mutex
	id     string
	ctx    context.Context
	cancel context.CancelFunc
}

var (
	_ entcache.Cache          = (*NatsKV)(nil)
	_ entcache.StampedeLocker = (*NatsKV)(nil)
	_ entcache.Invalidator    = (*NatsKV)(nil)
)

// New returns a new NATS JetStream KeyValue cache level.
func New(kv jetstream.KeyValue) *NatsKV {
	ctx, cancel := context.WithCancel(context.Background())
	return &NatsKV{
		kv:     kv,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Close gracefully tears down the heartbeat instance.
func (n *NatsKV) Close() {
	n.cancel()
	n.mu.Lock()
	id := n.id
	n.mu.Unlock()
	if id != "" {
		_ = n.kv.Delete(context.Background(), id)
	}
}

func (n *NatsKV) keepalive() string {
	n.mu.Lock()
	id := n.id
	n.mu.Unlock()

	if id == "" {
		id = "natsid_" + uuid.NewString()
		if rev, err := n.kv.Create(n.ctx, id, []byte("1"), jetstream.KeyTTL(10*time.Second)); err == nil {
			n.mu.Lock()
			if n.id == "" {
				n.id = id
				go n.refresh(id, rev)
			} else {
				id = n.id
			}
			n.mu.Unlock()
		}
	}
	return id
}

func (n *NatsKV) refresh(id string, rev uint64) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			// Update resets the TTL to whatever it was created with (10s).
			newRev, err := n.kv.Update(n.ctx, id, []byte("1"), rev)
			if err == nil {
				rev = newRev
			} else {
				// If Update fails (e.g., revision mismatch or it expired), recreate it.
				newRev, createErr := n.kv.Create(n.ctx, id, []byte("1"), jetstream.KeyTTL(10*time.Second))
				if createErr == nil {
					rev = newRev
				} else if errors.Is(createErr, jetstream.ErrKeyExists) {
					// Fetch new revision if someone else created it
					if entry, getErr := n.kv.Get(n.ctx, id); getErr == nil {
						rev = entry.Revision()
					}
				}
			}
		}
	}
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
	// Since data and lock are decoupled, we just write the data with the proper TTL.
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
	lockKey := key + "_lock"

	// Fast path: check if data already exists
	if kve, err := n.kv.Get(ctx, key); err == nil {
		buf := kve.Value()
		if len(buf) > 0 {
			e := &entcache.Entry{}
			if err := e.UnmarshalBinary(buf); err == nil {
				return false, func(c context.Context) (*entcache.Entry, error) {
					return e, nil
				}, nil, nil
			}
		}
	}

	placeholderID := n.keepalive()

	// Create lock key without a TTL. We rely exclusively on checking the liveness
	// of placeholderID in the wait() loop. A dead holder's lock will be purged by the next waiter.
	rev, err := n.kv.Create(ctx, lockKey, []byte(placeholderID))
	if err == nil {
		release := func(c context.Context) {
			_ = n.kv.Purge(c, lockKey, jetstream.LastRevision(rev))
		}
		return true, nil, release, nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return false, nil, nil, err
	}

	wait := func(c context.Context) (*entcache.Entry, error) {
		// Double check if data was written right before we got here
		if kve, err := n.kv.Get(c, key); err == nil {
			buf := kve.Value()
			if len(buf) > 0 {
				e := &entcache.Entry{}
				if err := e.UnmarshalBinary(buf); err == nil {
					return e, nil
				}
			}
		}

		// Look up who holds the lock so we can check their heartbeat
		kve, gErr := n.kv.Get(c, lockKey)
		if gErr != nil {
			if errors.Is(gErr, jetstream.ErrKeyNotFound) {
				return nil, entcache.ErrRetryLocker
			}
			return nil, gErr
		}

		holderID := string(kve.Value())
		holderRev := kve.Revision()

		if _, ckErr := n.kv.Get(c, holderID); ckErr != nil && errors.Is(ckErr, jetstream.ErrKeyNotFound) {
			_ = n.kv.Purge(c, lockKey, jetstream.LastRevision(holderRev))
			return nil, entcache.ErrRetryLocker
		}

		watcher, wErr := n.kv.Watch(c, lockKey)
		if wErr != nil {
			return nil, wErr
		}
		defer watcher.Stop()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.Done():
				return nil, c.Err()
			case <-ticker.C:
				if _, ckErr := n.kv.Get(c, holderID); ckErr != nil && errors.Is(ckErr, jetstream.ErrKeyNotFound) {
					_ = n.kv.Purge(c, lockKey, jetstream.LastRevision(holderRev))
					return nil, entcache.ErrRetryLocker
				}
				// Holder is still alive, keep watching!
			case update, ok := <-watcher.Updates():
				if !ok {
					return nil, entcache.ErrRetryLocker
				}
				if update != nil {
					op := update.Operation()
					if op == jetstream.KeyValuePurge || op == jetstream.KeyValueDelete {
						// Lock was cleared! Either the winner wrote the data or crashed.
						// Check if data is now available.
						if dataKve, err := n.kv.Get(c, key); err == nil {
							buf := dataKve.Value()
							if len(buf) > 0 {
								e := &entcache.Entry{}
								if err := e.UnmarshalBinary(buf); err == nil {
									return e, nil
								}
							}
						}
						return nil, entcache.ErrRetryLocker
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
					if op == jetstream.KeyValuePurge {
						continue
					}
					k := update.Key()
					if strings.HasPrefix(k, "natsid_") {
						continue
					}
					if op == jetstream.KeyValueDelete || op == jetstream.KeyValuePut {
						if !strings.HasSuffix(k, "_lock") {
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

// crash simulates an ungraceful shutdown by stopping the heartbeat without deleting the marker.
func (n *NatsKV) crash() {
	n.cancel()
}
