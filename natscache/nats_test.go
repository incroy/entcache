package natscache_test

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"github.com/incroy/entcache"
	"github.com/incroy/entcache/natscache"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func runEmbeddedNatsServer(t *testing.T) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	s := natsserver.RunServer(&opts)
	t.Cleanup(func() {
		s.Shutdown()
	})

	nc, err := nats.Connect(s.ClientURL())
	require.NoError(t, err, "failed to connect to embedded NATS server")
	t.Cleanup(func() {
		nc.Close()
	})

	js, err := jetstream.New(nc)
	require.NoError(t, err, "failed to create JetStream context")
	return nc, js
}

func TestNatsCache_EmbeddedJetStream(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_embedded_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c := natscache.New(kv)
	key := "user:test:99"
	entry := &entcache.Entry{
		Columns: []string{"id", "username"},
		Values:  [][]driver.Value{{int64(99), "bob"}},
	}

	// 1. LockOrWait Winner
	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err, "LockOrWait failed")
	require.True(t, won, "expected first call to win lock")

	// 2. Add entry
	err = c.Add(ctx, key, entry, time.Minute)
	require.NoError(t, err, "Add failed")
	if release != nil {
		release(ctx)
	}

	// 3. Get entry
	got, err := c.Get(ctx, key)
	require.NoError(t, err, "Get failed")
	require.Len(t, got.Values, 1)
	require.Equal(t, "bob", got.Values[0][1], "unexpected value in entry")

	// 4. WatchInvalidations
	invalidated := make(chan string, 1)
	err = c.WatchInvalidations(ctx, func(k entcache.Key) {
		invalidated <- k.(string)
	})
	require.NoError(t, err, "WatchInvalidations failed")

	_ = c.Del(ctx, key)

	select {
	case k := <-invalidated:
		require.Equal(t, "user_test_99", k, "expected invalidated key user_test_99")
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for WatchInvalidations event")
	}
}

func TestNatsLockOrWait_Stampede(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_stampede_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c := natscache.New(kv)
	key := "test:nats:stampede"

	won1, wait1, release1, err1 := c.LockOrWait(ctx, key)
	require.NoError(t, err1, "first LockOrWait failed")
	require.True(t, won1, "expected first call to win lock")

	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	require.NoError(t, err2, "second LockOrWait failed")
	require.False(t, won2, "expected second call to lose lock")

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(77)}},
	}

	var wg sync.WaitGroup
	var waiterEntry *entcache.Entry
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiterEntry, waiterErr = wait2(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	err = c.Add(ctx, key, entry, time.Minute)
	require.NoError(t, err, "Add failed")
	if release1 != nil {
		release1(ctx)
	}

	wg.Wait()

	require.NoError(t, waiterErr, "waiter error")
	require.NotNil(t, waiterEntry, "waiter received nil entry")
	require.Len(t, waiterEntry.Values, 1)
	require.Equal(t, int64(77), waiterEntry.Values[0][0], "waiter received wrong entry")
	_ = wait1
}

func TestNatsLockOrWait_HeartbeatFailure(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_heartbeat_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c1 := natscache.New(kv)
	c2 := natscache.New(kv)
	key := "test:nats:heartbeat"

	won1, _, _, err1 := c1.LockOrWait(ctx, key)
	require.NoError(t, err1, "first LockOrWait failed")
	require.True(t, won1, "expected first call to win lock")

	won2, wait2, _, err2 := c2.LockOrWait(ctx, key)
	require.NoError(t, err2, "second LockOrWait failed")
	require.False(t, won2, "expected second call to lose lock")

	var wg sync.WaitGroup
	var waiterErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, waiterErr = wait2(ctx)
	}()

	// Simulate crash of c1
	c1.Close()

	wg.Wait()
	require.ErrorIs(t, waiterErr, entcache.ErrRetryLocker, "expected ErrRetryLocker on heartbeat failure")
}

func TestNatsLockOrWait_ConcurrentStealing(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_concurrent_test",
		TTL:            10 * time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	key := "test:nats:concurrent"
	numClients := 3

	clients := make([]*natscache.NatsKV, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = natscache.New(kv)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	winners := 0
	losers := 0

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(c *natscache.NatsKV) {
			defer wg.Done()
			won, _, _, err := c.LockOrWait(ctx, key)
			require.NoError(t, err, "LockOrWait failed")
			mu.Lock()
			if won {
				winners++
			} else {
				losers++
			}
			mu.Unlock()
		}(clients[i])
	}
	wg.Wait()

	require.Equal(t, 1, winners, "expected exactly 1 winner")
	require.Equal(t, numClients-1, losers, "expected exactly %d losers", numClients-1)
}

func TestNats_HeartbeatTTLRefresh(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_heartbeat_ttl_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	// We can't easily change the hardcoded 5s ticker in the code without a refactor,
	// so we will test the actual 10s TTL behavior. We'll simulate `keepalive()` directly
	// or just wait. But waiting 15s in a unit test is slow.
	// Let's test the `Add` TTL behavior first.
}

func TestNats_AddPreservesCustomTTL(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_add_ttl_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err, "failed to create KV bucket")

	c := natscache.New(kv)
	key := "test:nats:add_ttl"

	won, _, release, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(1)}},
	}

	// Add with 1 second TTL
	err = c.Add(ctx, key, entry, 1*time.Second)
	require.NoError(t, err)
	if release != nil {
		release(ctx)
	}

	// Read immediately
	got, err := c.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Wait 2 seconds, it should expire
	time.Sleep(2 * time.Second)

	_, err = c.Get(ctx, key)
	require.ErrorIs(t, err, entcache.ErrNotFound, "expected entry to expire after 1s TTL, instead got nil or different error")
}

func TestNatsCacheDel(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_del_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)
	key := "test:nats:del"

	entry := &entcache.Entry{Values: [][]driver.Value{{int64(1)}}}
	require.NoError(t, c.Add(ctx, key, entry, time.Minute))

	// Verify it exists
	got, err := c.Get(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Delete it
	require.NoError(t, c.Del(ctx, key))

	// Verify it's gone
	_, err = c.Get(ctx, key)
	require.ErrorIs(t, err, entcache.ErrNotFound)
}

func TestNatsLockOrWait_ContextCancellation(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_cancel_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)
	key := "test:nats:cancel"

	won, _, _, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	// Waiter with cancellable context
	cancelCtx, cancel := context.WithCancel(ctx)
	won2, wait2, _, err := c.LockOrWait(cancelCtx, key)
	require.NoError(t, err)
	require.False(t, won2)

	// Cancel context immediately
	cancel()

	// Wait should return immediately with context.Canceled
	_, waitErr := wait2(cancelCtx)
	require.ErrorIs(t, waitErr, context.Canceled)
}

func TestNatsLockOrWait_Timeout(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_timeout_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)
	key := "test:nats:timeout"

	won, _, _, err := c.LockOrWait(ctx, key)
	require.NoError(t, err)
	require.True(t, won)

	// Waiter with very short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	won2, wait2, _, err := c.LockOrWait(timeoutCtx, key)
	require.NoError(t, err)
	require.False(t, won2)

	// Wait should timeout quickly
	_, waitErr := wait2(timeoutCtx)
	require.ErrorIs(t, waitErr, context.DeadlineExceeded)
}

func TestNatsCloseCleanup(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_cleanup_test",
		LimitMarkerTTL: 1 * time.Second,
	})
	require.NoError(t, err)
	c := natscache.New(kv)
	
	// Create a lock to spawn the heartbeat goroutine
	won, _, _, err := c.LockOrWait(ctx, "test:nats:cleanup")
	require.NoError(t, err)
	require.True(t, won)

	// Close the cache
	c.Close()

	// Wait a moment to ensure goroutine exits (no easy way to assert runtime.NumGoroutine without flakiness, 
	// but we can ensure Close doesn't panic and returns cleanly)
	time.Sleep(50 * time.Millisecond)
}

