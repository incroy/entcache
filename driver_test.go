package entcache_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"
	"time"

	entSql "entgo.io/ent/dialect/sql"
	"github.com/alicebob/miniredis/v2"
	"github.com/incroy/entcache"
	"github.com/incroy/entcache/contextcache"
	"github.com/incroy/entcache/lrucache"
	"github.com/incroy/entcache/natscache"
	"github.com/incroy/entcache/rediscache"
	"github.com/incroy/entcache/rueidiscache"
	"github.com/redis/rueidis"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-redis/redismock/v9"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestDriver_ContextLevel(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := sql.OpenDB(dialect.MySQL, db)

	t.Run("One", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.Levels(contextcache.New()))
		mock.ExpectQuery("SELECT id FROM users").
			WillReturnRows(
				sqlmock.NewRows([]string{"id"}).
					AddRow(1).
					AddRow(2).
					AddRow(3),
			)
		ctx := contextcache.NewContext(context.Background())
		expectQuery(ctx, t, drv, "SELECT id FROM users", []interface{}{int64(1), int64(2), int64(3)})
		expectQuery(ctx, t, drv, "SELECT id FROM users", []interface{}{int64(1), int64(2), int64(3)})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multi", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.Levels(contextcache.New()))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx1 := contextcache.NewContext(context.Background())
		expectQuery(ctx1, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		ctx2 := contextcache.NewContext(context.Background())
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		expectQuery(ctx2, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("TTL", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.Levels(contextcache.New()), entcache.TTL(-1))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := contextcache.NewContext(context.Background())
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDriver_Levels(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := sql.OpenDB(dialect.Postgres, db)

	t.Run("One", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.TTL(time.Second), entcache.Levels(lrucache.MustNew(100)))
		mock.ExpectQuery("SELECT age FROM users").
			WillReturnRows(
				sqlmock.NewRows([]string{"age"}).
					AddRow(20.1).
					AddRow(30.2).
					AddRow(40.5),
			)
		expectQuery(context.Background(), t, drv, "SELECT age FROM users", []interface{}{20.1, 30.2, 40.5})
		expectQuery(context.Background(), t, drv, "SELECT age FROM users", []interface{}{20.1, 30.2, 40.5})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multi", func(t *testing.T) {
		drv := entcache.NewDriver(
			drv,
			entcache.Levels(
				lrucache.MustNew(10),
				lrucache.MustNew(100),
			),
		)
		mock.ExpectQuery("SELECT age FROM users").
			WillReturnRows(
				sqlmock.NewRows([]string{"age"}).
					AddRow(20.1).
					AddRow(30.2).
					AddRow(40.5),
			)
		expectQuery(context.Background(), t, drv, "SELECT age FROM users", []interface{}{20.1, 30.2, 40.5})
		expectQuery(context.Background(), t, drv, "SELECT age FROM users", []interface{}{20.1, 30.2, 40.5})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Redis", func(t *testing.T) {
		var (
			rdb, rmock = redismock.NewClientMock()
			drv        = entcache.NewDriver(
				drv,
				entcache.Levels(
					rediscache.New(rdb),
				),
				entcache.Hash(func(string, []interface{}) (entcache.Key, error) {
					return 1, nil
				}),
			)
		)
		mock.ExpectQuery("SELECT active FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"active"}).AddRow(true).AddRow(false))
		rmock.ExpectGet("1").RedisNil()
		buf, _ := entcache.Entry{Columns: []string{"active"}, Values: [][]driver.Value{{true}, {false}}}.MarshalBinary()
		rmock.ExpectSet("1", buf, 0).SetVal("OK")
		expectQuery(context.Background(), t, drv, "SELECT active FROM users", []interface{}{true, false})
		rmock.ExpectGet("1").SetVal(string(buf))
		expectQuery(context.Background(), t, drv, "SELECT active FROM users", []interface{}{true, false})
		if err := rmock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		expected := entcache.Stats{Gets: 2, Hits: 1}
		if s := drv.Stats(); s != expected {
			t.Errorf("unexpected stats: %v != %v", s, expected)
		}
	})
}

func TestDriver_ContextOptions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := sql.OpenDB(dialect.MySQL, db)

	t.Run("Skip", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.Levels(lrucache.MustNew(100)))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := context.Background()
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		skipCtx := entcache.Skip(ctx)
		expectQuery(skipCtx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Evict", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.Levels(lrucache.MustNew(100)))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := context.Background()
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		evictCtx := entcache.Evict(ctx)
		expectQuery(evictCtx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("WithTTL", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.Levels(lrucache.MustNew(100)))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ttlCtx := entcache.WithTTL(context.Background(), -1)
		expectQuery(ttlCtx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		expectQuery(ttlCtx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("WithKey", func(t *testing.T) {
		drv := entcache.NewDriver(drv, entcache.Levels(lrucache.MustNew(100)))
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		ctx := context.Background()
		keyCtx := entcache.WithKey(ctx, "cache-key")
		expectQuery(keyCtx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		expectQuery(keyCtx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		if err := drv.Cache.Del(ctx, "cache-key"); err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("SELECT name FROM users").
			WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("a8m"))
		expectQuery(keyCtx, t, drv, "SELECT name FROM users", []interface{}{"a8m"})
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		expected := entcache.Stats{Gets: 4, Hits: 1}
		if s := drv.Stats(); s != expected {
			t.Errorf("unexpected stats: %v != %v", s, expected)
		}
	})
}

func TestDriver_SkipInsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := entcache.NewDriver(sql.OpenDB(dialect.Postgres, db), entcache.Hash(func(string, []interface{}) (entcache.Key, error) {
		t.Fatal("Driver.Query should not be called for INSERT statements")
		return nil, nil
	}))
	mock.ExpectQuery("INSERT INTO users DEFAULT VALUES RETURNING id").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	expectQuery(context.Background(), t, drv, "INSERT INTO users DEFAULT VALUES RETURNING id", []interface{}{int64(1)})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	var expected entcache.Stats
	if s := drv.Stats(); s != expected {
		t.Errorf("unexpected stats: %v != %v", s, expected)
	}
}

func expectQuery(ctx context.Context, t *testing.T, drv dialect.Driver, query string, args []interface{}) {
	rows := &sql.Rows{}
	if err := drv.Query(ctx, query, []interface{}{}, rows); err != nil {
		t.Fatalf("unexpected query failure: %q: %v", query, err)
	}
	var dest []interface{}
	for rows.Next() {
		cols, err := rows.Columns()
		if err != nil || len(cols) <= 1 {
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				t.Fatal("unexpected Rows.Scan failure:", err)
			}
			dest = append(dest, v)
		} else {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal("unexpected Rows.Scan failure:", err)
			}
			dest = append(dest, vals...)
		}
	}
	if len(dest) != len(args) {
		t.Fatalf("mismatch rows length: %d != %d", len(dest), len(args))
	}
	for i := range dest {
		if dest[i] != args[i] {
			t.Fatalf("mismatch values: %v != %v", dest[i], args[i])
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDriver_ChangeSetAndEntryKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	cs := entcache.NewChangeSet(time.Minute)
	cs.Start()
	defer cs.Stop()

	drv := entcache.NewDriver(
		sql.OpenDB(dialect.Postgres, db),
		entcache.WithKeyTTL(time.Hour),
		entcache.WithChangeSet(cs),
		entcache.Levels(lrucache.MustNew(100)),
	)

	ctx := context.Background()
	entryCtx := entcache.WithEntryKey(ctx, "User", 42)

	// First query: Miss -> DB Query -> Cached
	mock.ExpectQuery("SELECT id, name FROM users WHERE id = 42").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(42), "alice"))
	expectQuery(entryCtx, t, drv, "SELECT id, name FROM users WHERE id = 42", []interface{}{int64(42), "alice"})

	// Second query: Hit from Cache
	expectQuery(entryCtx, t, drv, "SELECT id, name FROM users WHERE id = 42", []interface{}{int64(42), "alice"})

	// Mark User:42 as mutated in ChangeSet
	cs.Mark("User:42")

	// Third query: ChangeSet detects mutation -> Evicts & Re-queries DB
	mock.ExpectQuery("SELECT id, name FROM users WHERE id = 42").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(42), "alice-updated"))
	expectQuery(entryCtx, t, drv, "SELECT id, name FROM users WHERE id = 42", []interface{}{int64(42), "alice-updated"})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDriver_SkipNotFoundOption(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	drv := entcache.NewDriver(
		sql.OpenDB(dialect.Postgres, db),
		entcache.Levels(lrucache.MustNew(100)),
	)

	ctx := entcache.SkipNotFound(context.Background())

	// Query returns zero rows
	mock.ExpectQuery("SELECT id FROM users WHERE id = 999").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	expectQuery(ctx, t, drv, "SELECT id FROM users WHERE id = 999", []interface{}{})

	// Since SkipNotFound was set and zero rows returned, second query MUST hit DB again (not cached)
	mock.ExpectQuery("SELECT id FROM users WHERE id = 999").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	expectQuery(ctx, t, drv, "SELECT id FROM users WHERE id = 999", []interface{}{})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDriver_NatsCache(t *testing.T) {
	ctx := context.Background()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	s := natsserver.RunServer(&opts)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to embedded NATS server: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("failed to create JetStream: %v", err)
	}

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:         "entcache_driver_test",
		TTL:            time.Minute,
		LimitMarkerTTL: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create KV bucket: %v", err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	natsBackend := natscache.New(kv)
	drv := entcache.NewDriver(
		sql.OpenDB(dialect.Postgres, db),
		entcache.TTL(time.Minute),
		entcache.Levels(natsBackend),
	)

	// First Query: Miss -> DB Query -> Cached in NATS KV
	mock.ExpectQuery("SELECT id, name FROM users WHERE id = 1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(1), "bob"))
	expectQuery(ctx, t, drv, "SELECT id, name FROM users WHERE id = 1", []interface{}{int64(1), "bob"})

	// Second Query: Hit directly from NATS KV Cache
	expectQuery(ctx, t, drv, "SELECT id, name FROM users WHERE id = 1", []interface{}{int64(1), "bob"})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDriver_StampedeLock(t *testing.T) {
	db, sqlMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}

	// Expect exactly 1 query to database, because stampede protection prevents duplicate DB calls
	sqlMock.ExpectQuery("SELECT name FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow("alice"))

	drv := entcache.NewDriver(
		sql.OpenDB(dialect.Postgres, db),
		entcache.Levels(lrucache.MustNew(100)),
		entcache.TTL(time.Minute),
	)

	ctx := context.Background()
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"alice"})
	}()

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		expectQuery(ctx, t, drv, "SELECT name FROM users", []interface{}{"alice"})
	}()

	wg.Wait()

	if err := sqlMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDriver_ReleaseOnScanError(t *testing.T) {
	db, sqlMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	drv := entcache.NewDriver(
		sql.OpenDB(dialect.Postgres, db),
		entcache.Levels(contextcache.New()),
		entcache.Hash(func(string, []interface{}) (entcache.Key, error) {
			return "query:key:1", nil
		}),
	)

	// First query (Winner) will fail during scanning!
	sqlMock.ExpectQuery("SELECT id FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(1)).RowError(0, fmt.Errorf("mid-scan panic/error")))

	ctx := context.Background()

	// Execute the query
	rows := &entSql.Rows{}
	err = drv.Query(ctx, "SELECT id FROM users", []interface{}{}, rows)
	if err != nil {
		t.Fatalf("expected query to succeed and return rows, got err: %v", err)
	}

	// Read rows (this is where the error happens!)
	for rows.Next() {
		// Do nothing
	}
	if rows.Err() == nil || rows.Err().Error() != "mid-scan panic/error" {
		t.Fatalf("expected mid-scan error, got: %v", rows.Err())
	}

	// The ORM defers rows.Close() which calls recorder.Close().
	// THIS is what was previously skipping releaseFunc because rows.Err() != nil.
	_ = rows.Close()

	// Waiter: If the lock leaked, this will block forever.
	// If the fix in recorder.Close() works, the lock is released, and we can query again!
	sqlMock.ExpectQuery("SELECT id FROM users").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(2)))

	ctxTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	rows2 := &entSql.Rows{}
	err = drv.Query(ctxTimeout, "SELECT id FROM users", []interface{}{}, rows2)
	if err != nil {
		t.Fatalf("waiter failed to acquire lock (did it leak?): %v", err)
	}
	_ = rows2.Close()

	if err := sqlMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDriver_RueidisCache(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{s.Addr()},
		DisableCache: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	db, sqlMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rueidisBackend := rueidiscache.New(client)
	drv := entcache.NewDriver(
		sql.OpenDB(dialect.Postgres, db),
		entcache.TTL(time.Minute),
		entcache.Levels(rueidisBackend),
	)

	// First query: Miss -> DB Query -> Cached in Rueidis
	sqlMock.ExpectQuery("SELECT id, name FROM users WHERE id = 2").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(int64(2), "alice"))
	expectQuery(ctx, t, drv, "SELECT id, name FROM users WHERE id = 2", []interface{}{int64(2), "alice"})

	// Second query: Hit directly from Rueidis Cache
	expectQuery(ctx, t, drv, "SELECT id, name FROM users WHERE id = 2", []interface{}{int64(2), "alice"})

	if err := sqlMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
