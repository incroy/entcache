package entcache

import (
	"sync"
	"time"
)

const defaultGCInterval = 5 * time.Minute

// ChangeSet tracks entity keys that have been modified (created, updated, or
// deleted). It is used by the Driver to detect stale cache entries and force
// re-queries. A background GC goroutine prunes entries older than the GC interval.
type ChangeSet struct {
	mu         sync.RWMutex
	changes    map[string]time.Time
	gcInterval time.Duration
	stopCh     chan struct{}
}

// NewChangeSet creates a new ChangeSet with the given GC interval.
// If gcInterval is <= 0, the default of 5 minutes is used.
func NewChangeSet(gcInterval time.Duration) *ChangeSet {
	if gcInterval <= 0 {
		gcInterval = defaultGCInterval
	}
	return &ChangeSet{
		changes:    make(map[string]time.Time),
		gcInterval: gcInterval,
	}
}

// Mark records one or more keys as changed at the current time.
func (cs *ChangeSet) Mark(keys ...Key) {
	now := time.Now()
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, k := range keys {
		if s, ok := k.(string); ok {
			cs.changes[s] = now
		}
	}
}

// Changed reports whether the given key has been marked as changed
// since the given time. This is used by the Driver to decide whether
// a cache hit should be evicted and re-fetched.
func (cs *ChangeSet) Changed(key Key, since time.Time) bool {
	s, ok := key.(string)
	if !ok {
		return false
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	t, exists := cs.changes[s]
	if !exists {
		return false
	}
	return t.After(since)
}

// Clear removes the change markers for the given keys, acknowledging
// that the cache has been refreshed.
func (cs *ChangeSet) Clear(keys ...Key) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, k := range keys {
		if s, ok := k.(string); ok {
			delete(cs.changes, s)
		}
	}
}

// Start begins the background GC goroutine that prunes stale change
// markers. Call Stop to terminate it.
func (cs *ChangeSet) Start() {
	cs.stopCh = make(chan struct{})
	go cs.gc()
}

// Stop terminates the background GC goroutine.
func (cs *ChangeSet) Stop() {
	if cs.stopCh != nil {
		close(cs.stopCh)
	}
}

func (cs *ChangeSet) gc() {
	ticker := time.NewTicker(cs.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-cs.stopCh:
			return
		case now := <-ticker.C:
			cutoff := now.Add(-cs.gcInterval)
			cs.mu.Lock()
			for k, t := range cs.changes {
				if t.Before(cutoff) {
					delete(cs.changes, k)
				}
			}
			cs.mu.Unlock()
		}
	}
}
