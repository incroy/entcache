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
	if err != nil {
		t.Fatalf("failed to connect to embedded NATS server: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
	})

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("failed to create JetStream context: %v", err)
	}
	return nc, js
}

func TestNatsCache_EmbeddedJetStream(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "entcache_embedded_test",
		TTL:    10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create KV bucket: %v", err)
	}

	c := natscache.New(kv)
	key := "user:test:99"
	entry := &entcache.Entry{
		Columns: []string{"id", "username"},
		Values:  [][]driver.Value{{int64(99), "bob"}},
	}

	// 1. LockOrWait Winner
	won, _, release, err := c.LockOrWait(ctx, key)
	if err != nil {
		t.Fatalf("LockOrWait failed: %v", err)
	}
	if !won {
		t.Fatalf("expected first call to win lock")
	}

	// 2. Add entry
	if err := c.Add(ctx, key, entry, time.Minute); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if release != nil {
		release(ctx)
	}

	// 3. Get entry
	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(got.Values) != 1 || got.Values[0][1] != "bob" {
		t.Errorf("unexpected value in entry: %v", got.Values)
	}

	// 4. WatchInvalidations
	invalidated := make(chan string, 1)
	err = c.WatchInvalidations(ctx, func(k entcache.Key) {
		invalidated <- k.(string)
	})
	if err != nil {
		t.Fatalf("WatchInvalidations failed: %v", err)
	}

	_ = c.Del(ctx, key)

	select {
	case k := <-invalidated:
		if k != "user_test_99" {
			t.Errorf("expected invalidated key user_test_99, got %s", k)
		}
	case <-time.After(3 * time.Second):
		t.Errorf("timeout waiting for WatchInvalidations event")
	}
}

func TestNatsLockOrWait_Stampede(t *testing.T) {
	ctx := context.Background()
	_, js := runEmbeddedNatsServer(t)

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "entcache_stampede_test",
		TTL:    10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create KV bucket: %v", err)
	}

	c := natscache.New(kv)
	key := "test:nats:stampede"

	won1, wait1, release1, err1 := c.LockOrWait(ctx, key)
	if err1 != nil {
		t.Fatalf("first LockOrWait failed: %v", err1)
	}
	if !won1 {
		t.Fatalf("expected first call to win lock")
	}

	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	if err2 != nil {
		t.Fatalf("second LockOrWait failed: %v", err2)
	}
	if won2 {
		t.Fatalf("expected second call to lose lock")
	}

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
	if err := c.Add(ctx, key, entry, time.Minute); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if release1 != nil {
		release1(ctx)
	}

	wg.Wait()

	if waiterErr != nil {
		t.Fatalf("waiter error: %v", waiterErr)
	}
	if waiterEntry == nil || len(waiterEntry.Values) == 0 || waiterEntry.Values[0][0] != int64(77) {
		t.Fatalf("waiter received wrong entry: %v", waiterEntry)
	}
	_ = wait1
}
