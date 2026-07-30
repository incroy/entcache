package rueidiscache_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/incroy/entcache"
	"github.com/incroy/entcache/rueidiscache"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

func TestRueidisCache_Get_Hit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	entry := &entcache.Entry{
		Columns: []string{"id", "name"},
		Values:  [][]driver.Value{{int64(10), "alice"}},
	}
	buf, err := entry.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal entry: %v", err)
	}

	cmd := mockClient.B().Get().Key("user:10").Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisString(string(buf))))

	got, err := c.Get(ctx, "user:10")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !reflect.DeepEqual(got.Columns, entry.Columns) {
		t.Errorf("expected columns %v, got %v", entry.Columns, got.Columns)
	}
	if len(got.Values) != 1 || got.Values[0][1] != "alice" {
		t.Errorf("expected values %v, got %v", entry.Values, got.Values)
	}
}

func TestRueidisCache_Get_Miss(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	cmd := mockClient.B().Get().Key("user:miss").Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisNil()))

	_, err := c.Get(ctx, "user:miss")
	if !errors.Is(err, entcache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestRueidisCache_Get_RedisError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	cmd := mockClient.B().Get().Key("user:err").Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisError("connection failed")))

	_, err := c.Get(ctx, "user:err")
	if !errors.Is(err, entcache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestRueidisCache_Get_EmptyKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	_, err := c.Get(ctx, "")
	if !errors.Is(err, entcache.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for empty key, got %v", err)
	}
}

func TestRueidisCache_Get_CorruptedData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	cmd := mockClient.B().Get().Key("user:corrupt").Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisString("invalid-binary-data")))

	_, err := c.Get(ctx, "user:corrupt")
	if err == nil {
		t.Fatalf("expected error for corrupted binary data, got nil")
	}
	if errors.Is(err, entcache.ErrNotFound) {
		t.Fatalf("expected unmarshal error, got ErrNotFound")
	}
}

func TestRueidisCache_Add_WithTTL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(1)}},
	}
	buf, _ := entry.MarshalBinary()

	cmd := mockClient.B().Set().Key("user:10").Value(rueidis.BinaryString(buf)).Ex(time.Minute).Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisString("OK")))

	if err := c.Add(ctx, "user:10", entry, time.Minute); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
}

func TestRueidisCache_Add_WithoutTTL(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(2)}},
	}
	buf, _ := entry.MarshalBinary()

	cmd := mockClient.B().Set().Key("user:20").Value(rueidis.BinaryString(buf)).Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisString("OK")))

	if err := c.Add(ctx, "user:20", entry, 0); err != nil {
		t.Fatalf("Add without TTL failed: %v", err)
	}
}

func TestRueidisCache_Add_EmptyKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	entry := &entcache.Entry{}
	if err := c.Add(ctx, "", entry, time.Minute); err != nil {
		t.Fatalf("expected nil error for empty key, got %v", err)
	}
}

func TestRueidisCache_Add_RedisError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(3)}},
	}

	mockClient.EXPECT().Do(ctx, gomock.Any()).Return(mock.Result(mock.RedisError("redis out of memory")))

	if err := c.Add(ctx, "user:30", entry, time.Minute); err == nil {
		t.Fatalf("expected error on Redis set failure, got nil")
	}
}

func TestRueidisCache_Del(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	cmd := mockClient.B().Del().Key("user:10").Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisInt64(1)))

	if err := c.Del(ctx, "user:10"); err != nil {
		t.Fatalf("Del failed: %v", err)
	}
}

func TestRueidisCache_Del_EmptyKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	if err := c.Del(ctx, ""); err != nil {
		t.Fatalf("expected nil for empty key, got %v", err)
	}
}

func TestRueidisCache_Del_RedisError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()

	cmd := mockClient.B().Del().Key("user:err").Build()
	mockClient.EXPECT().Do(ctx, cmd).Return(mock.Result(mock.RedisError("del failed")))

	if err := c.Del(ctx, "user:err"); err == nil {
		t.Fatalf("expected error on Del failure, got nil")
	}
}

func TestRueidisLockOrWait(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	ctx := context.Background()
	key := "test:lock:1"

	won1, _, release1, err1 := c.LockOrWait(ctx, key)
	if err1 != nil {
		t.Fatalf("first LockOrWait failed: %v", err1)
	}
	if !won1 {
		t.Errorf("expected first call to win lock")
	}

	won2, wait2, _, err2 := c.LockOrWait(ctx, key)
	if err2 != nil {
		t.Fatalf("second LockOrWait failed: %v", err2)
	}
	if won2 {
		t.Errorf("expected second call to lose lock and become waiter")
	}

	entry := &entcache.Entry{
		Columns: []string{"id"},
		Values:  [][]driver.Value{{int64(100)}},
	}
	buf, _ := entry.MarshalBinary()

	setCmd := mockClient.B().Set().Key(key).Value(rueidis.BinaryString(buf)).Ex(time.Minute).Build()
	getCmd := mockClient.B().Get().Key(key).Build()

	mockClient.EXPECT().Do(ctx, setCmd).Return(mock.Result(mock.RedisString("OK")))
	mockClient.EXPECT().Do(ctx, getCmd).Return(mock.Result(mock.RedisString(string(buf))))

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
		t.Fatalf("waiter returned error: %v", waiterErr)
	}
	if waiterEntry == nil || len(waiterEntry.Values) == 0 || waiterEntry.Values[0][0] != int64(100) {
		t.Errorf("waiter did not receive expected entry: %v", waiterEntry)
	}
}

func TestRueidisLockOrWait_ContextCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := mock.NewClient(ctrl)
	c := rueidiscache.New(mockClient)
	key := "test:lock:cancel"

	won1, _, release1, _ := c.LockOrWait(context.Background(), key)
	if !won1 {
		t.Fatalf("expected winner")
	}
	defer release1(context.Background())

	cancelCtx, cancel := context.WithCancel(context.Background())
	_, wait2, _, _ := c.LockOrWait(cancelCtx, key)

	cancel()

	_, err := wait2(cancelCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
