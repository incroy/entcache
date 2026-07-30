package entcache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

// Rueidis provides a remote cache backed by rueidis and implements the
// AddGetDeleter interface with stampede protection.
//
// Stampede protection (inspired by rueidisaside): when a cache miss occurs,
// the first caller for a given key proceeds to fetch from the database while
// concurrent callers for the same key block on a channel until the first
// caller populates the cache. This prevents multiple identical database
// queries from being executed simultaneously.
type Rueidis struct {
	c     rueidis.Client
	mu    sync.Mutex
	waits map[string]chan struct{}
}

// NewRueidis returns a new Rueidis cache level from the given rueidis client.
//
//	c, _ := rueidis.NewClient(rueidis.ClientOption{
//		InitAddress: []string{"127.0.0.1:6379"},
//	})
//	entcache.NewRueidis(c)
func NewRueidis(c rueidis.Client) *Rueidis {
	return &Rueidis{
		c:     c,
		waits: make(map[string]chan struct{}),
	}
}

// Add adds the entry to the cache. If a stampede wait channel exists for
// this key, it is closed to unblock any goroutines waiting on Get.
func (r *Rueidis) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
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
	// Unblock any goroutines waiting for this key (stampede protection).
	r.mu.Lock()
	if ch, ok := r.waits[key]; ok {
		close(ch)
		delete(r.waits, key)
	}
	r.mu.Unlock()
	return nil
}

// Get gets an entry from the cache. If the key is not found, it returns
// ErrNotFound. Callers can use the stampede protection via the Driver's
// singleflight integration — this method itself is non-blocking.
func (r *Rueidis) Get(ctx context.Context, k Key) (*Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, ErrNotFound
	}
	cmd := r.c.B().Get().Key(key).Build()
	resp := r.c.Do(ctx, cmd)
	if err := resp.Error(); err != nil {
		return nil, ErrNotFound
	}
	buf, err := resp.AsBytes()
	if err != nil || len(buf) == 0 {
		return nil, ErrNotFound
	}
	e := &Entry{}
	if err := e.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return e, nil
}

// Del deletes an entry from the cache.
func (r *Rueidis) Del(ctx context.Context, k Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	cmd := r.c.B().Del().Key(key).Build()
	return r.c.Do(ctx, cmd).Error()
}

// Register registers interest in a key for stampede protection. Returns a
// channel that will be closed when the key is populated via Add, and a
// boolean indicating whether this caller is the first (i.e. should fetch).
// If first is true, the caller is responsible for calling Add or Unregister.
func (r *Rueidis) Register(key string) (wait <-chan struct{}, first bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ch, ok := r.waits[key]; ok {
		return ch, false
	}
	ch := make(chan struct{})
	r.waits[key] = ch
	return ch, true
}

// Unregister removes a stampede wait channel without populating the cache.
// Use this when the first caller encounters an error fetching from the database.
func (r *Rueidis) Unregister(key string) {
	r.mu.Lock()
	if ch, ok := r.waits[key]; ok {
		close(ch)
		delete(r.waits, key)
	}
	r.mu.Unlock()
}
