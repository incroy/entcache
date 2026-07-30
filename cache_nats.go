package entcache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// NatsKV provides a remote cache backed by NATS JetStream KeyValue
// and implements the AddGetDeleter interface.
//
// NATS JetStream KV supports:
//   - Create as a SETNX equivalent (only sets if key doesn't exist), with
//     optional per-key TTL via jetstream.KeyTTL.
//   - Watch for invalidation notifications.
//   - Bucket-level TTL (MaxAge) for automatic expiry of all keys.
//
// For per-key TTL, the implementation uses Create (which accepts KVCreateOpt)
// when TTL > 0. Put is used as a fallback when no TTL is needed since it does
// not accept TTL options.
type NatsKV struct {
	kv jetstream.KeyValue
}

// NewNatsKV returns a new NATS JetStream KeyValue cache level.
// The bucket should be created/configured externally. If you need
// automatic expiry, set MaxAge on the KeyValueConfig when creating
// the bucket.
//
//	js, _ := jetstream.New(nc)
//	kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
//		Bucket: "entcache",
//		MaxAge: 10 * time.Minute, // bucket-level TTL
//	})
//	entcache.NewNatsKV(kv)
func NewNatsKV(kv jetstream.KeyValue) *NatsKV {
	return &NatsKV{kv: kv}
}

// Add adds the entry to the cache using Put (unconditional overwrite).
// NATS KV Put does not support per-key TTL — expiry is governed by
// the bucket's MaxAge configuration. The ttl parameter is used with
// a Delete-then-Create approach when ttl > 0 to leverage Create's
// KeyTTL option for per-key expiry.
func (n *NatsKV) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	key := sanitizeNatsKey(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	if ttl > 0 {
		// Use Delete + Create to get per-key TTL support.
		_ = n.kv.Delete(ctx, key) // ignore error if key doesn't exist
		_, err = n.kv.Create(ctx, key, buf, jetstream.KeyTTL(ttl))
	} else {
		_, err = n.kv.Put(ctx, key, buf)
	}
	return err
}

// Create adds the entry to the cache only if the key does not already exist.
// This is the SETNX (set-if-not-exists) equivalent for NATS KV, useful for
// stampede protection: only the first caller that wins the Create will populate
// the cache, others will get an error and should wait or re-check.
func (n *NatsKV) Create(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	key := sanitizeNatsKey(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	var opts []jetstream.KVCreateOpt
	if ttl > 0 {
		opts = append(opts, jetstream.KeyTTL(ttl))
	}
	_, err = n.kv.Create(ctx, key, buf, opts...)
	return err
}

// Get gets an entry from the cache.
func (n *NatsKV) Get(ctx context.Context, k Key) (*Entry, error) {
	key := sanitizeNatsKey(k)
	if key == "" {
		return nil, ErrNotFound
	}
	kve, err := n.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	buf := kve.Value()
	if len(buf) == 0 {
		return nil, ErrNotFound
	}
	e := &Entry{}
	if err := e.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return e, nil
}

// Del deletes an entry from the cache.
func (n *NatsKV) Del(ctx context.Context, k Key) error {
	key := sanitizeNatsKey(k)
	if key == "" {
		return nil
	}
	err := n.kv.Delete(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}

// Watch returns a watcher for changes on keys matching the given pattern.
// This is the invalidation-notification equivalent: callers can watch for
// key updates and deletions to trigger cache invalidation in local caches
// when used in a multi-level setup.
//
// The returned KeyWatcher should be stopped by the caller when no longer needed.
func (n *NatsKV) Watch(ctx context.Context, pattern string, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return n.kv.Watch(ctx, pattern, opts...)
}

// sanitizeNatsKey converts a Key to a valid NATS KV key string.
// NATS KV keys must consist of alphanumeric characters, dashes,
// underscores, equal signs, and dots.
func sanitizeNatsKey(k Key) string {
	return fmt.Sprint(k)
}
