# entcache

[![Go Reference](https://pkg.go.dev/badge/github.com/incroy/entcache.svg)](https://pkg.go.dev/github.com/incroy/entcache)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A production-ready cache driver for [ent](https://github.com/ent/ent) with a variety of storage options and cache-aside strategies:

- **Context-level** — per-request cache attached to a `context.Context` (e.g. HTTP request or GraphQL resolve) that eliminates duplicate queries within a single request.
- **Driver-level** — process-level cache embedded in the `ent.Client` and shared across all goroutines.
- **Remote-level** — persistent, shared cache backed by [go-redis](https://github.com/redis/go-redis), [rueidis](https://github.com/redis/rueidis) (with stampede protection), or [NATS JetStream KV](https://docs.nats.io/nats-concepts/jetstream/key-value-store).
- **Multi-level** — hierarchical cache structure (e.g. L1 LRU memory cache + L2 remote Redis/NATS store) for optimal latency and durability.
- **Mutation-aware invalidation** — ent hooks automatically invalidate stale cache entries when entity mutations (create, update, delete) occur.

Compatible with standard `database/sql` drivers as well as native drivers like [entpgx](https://github.com/incroy/entpgx).

## Installation

```shell
go get github.com/incroy/entcache
```

## Quick Start

### With `database/sql` Driver

```go
// Open the database connection.
db, err := sql.Open(dialect.Postgres, "postgres://localhost:5432/mydb?sslmode=disable")
if err != nil {
    log.Fatal("opening database", err)
}

// Wrap the sql.Driver with entcache.Driver.
drv := entcache.NewDriver(
    sql.OpenDB(dialect.Postgres, db),
    entcache.TTL(time.Minute),
)

// Create an ent.Client.
client := ent.NewClient(ent.Driver(drv))

// Skip the cache during schema migration.
if err := client.Schema.Create(entcache.Skip(ctx)); err != nil {
    log.Fatal("running schema migration", err)
}

// First call hits the database.
u, err := client.User.Get(ctx, id)

// Second call is served from cache.
u, err = client.User.Get(ctx, id)
```

### With `entpgx` (Native pgx Driver)

[entpgx](https://github.com/incroy/entpgx) provides a native `pgxpool.Pool`-based `dialect.Driver` that bypasses `database/sql` entirely. `entcache` wraps it seamlessly:

```go
import (
    "github.com/incroy/entpgx"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/incroy/entcache"
)

// Create a pgxpool.Pool.
pool, err := pgxpool.New(ctx, "postgres://localhost:5432/mydb?sslmode=disable")
if err != nil {
    log.Fatal(err)
}

// Create the entpgx driver.
pgxDrv := entpgx.NewDriver(pool)

// Wrap with entcache. entcache works with any dialect.Driver.
drv := entcache.NewDriver(
    pgxDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(
        entcache.NewLRU(1024),
    ),
)

// Create an ent.Client.
client := ent.NewClient(ent.Driver(drv))

// Skip cache during migrations.
if err := client.Schema.Create(entcache.Skip(ctx)); err != nil {
    log.Fatal(err)
}

// Queries are cached transparently.
u, err := client.User.Get(ctx, id)
```

## High Level Design

On a high level, `entcache.Driver` decorates the `Query` method of the given driver, and for each call, generates a cache key (i.e. hash) from its arguments (statement and query parameters). After the query is executed, the driver records the raw values of the returned rows (`sql.Rows`) and stores them in the cache store with the generated cache key. Subsequent identical queries replay the recorded rows directly from cache without hitting the database, provided the entry has not expired or been evicted.

```
+------------+       1. Query(SQL, Args)       +------------------+       2. Get(Key)       +-------------+
| ent.Client | ------------------------------> | entcache.Driver  | ----------------------> | Cache Store |
+------------+                                 +------------------+                         +-------------+
                                                         |                                         |
                                                         | (Cache Miss)                            | (Hit)
                                                         v                                         v
                                               +------------------+                       +-----------------+
                                               | Wrapped Driver   |                       | Replay Cached   |
                                               | (SQL Database)   |                       | Rows            |
                                               +------------------+                       +-----------------+
```

The package provides a rich set of options to configure entry TTLs, control hash functions, set up multi-level cache hierarchies, invalidate/skip entries on-demand, and perform automatic mutation-aware invalidation.

## Caching Levels

`entcache` provides several builtin cache levels:

1. **`context.Context` Cache** — Attached to a request (e.g. HTTP request or GraphQL resolve). Used to eliminate duplicate database queries executed during the same request lifecycle.
2. **Driver-Level Cache** — Embedded in `ent.Client`. Shared across all goroutines in the application process.
3. **Remote-Level Cache** — Remote cache (Redis, Rueidis, NATS KV) providing persistence and sharing cache entries across multiple service replicas.
4. **Multi-Level Cache** — Hierarchical cache structure combining fast in-memory LRU caching with remote persistent backends.

---

### Context-Level Cache

Scoped to a single `context.Context` (e.g. `*http.Request`). The context carries an LRU cache (configurable) to eliminate duplicate database queries executed during the same request lifecycle.

This option is ideal for applications that require strong data consistency while preventing duplicate database queries within a request. For example, given the following GraphQL query:

```graphql
query($ids: [ID!]!) {
    nodes(ids: $ids) {
        ... on User {
            id
            name
            todos {
                id
                owner {
                    id
                    name
                }
            }
        }
    }
}
```

A naive GraphQL resolver executes 1 query for fetching $N$ users, $N$ queries for fetching todos of each user, and another query for each todo item to fetch its owner (the classic [_N+1 Problem_](https://entgo.io/docs/tutorial-todo-gql-field-collection/#problem)).

Ent optimizes this by batching execution into 3 queries:
1. Fetch $N$ users
2. Fetch todo items for **all** users
3. Fetch owners of **all** todo items

With `entcache`, the number of queries is further reduced from 3 to **2**, because the 1st query (fetching users) and 3rd query (fetching owners of todos) execute identical SQL statements, allowing the 3rd query to be served directly from the request-level cache.

![context-level-cache](https://github.com/ariga/entcache/blob/assets/internal/assets/ctxlevel.png)

#### Usage In GraphQL

Instantiate `entcache.Driver` with `ContextLevel()`:

```go
drv := entcache.NewDriver(sqlDrv, entcache.ContextLevel())
client := ent.NewClient(ent.Driver(drv))
```

Wrap the request `context.Context` with `entcache.NewContext` when a GraphQL query arrives:

```go
// GraphQL middleware
srv.AroundResponses(func(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
    if op := graphql.GetOperationContext(ctx).Operation; op != nil && op.Operation == ast.Query {
        ctx = entcache.NewContext(ctx)
    }
    return next(ctx)
})
```

#### HTTP Middleware Example

```go
srv.Use(func(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.Method == http.MethodGet {
            r = r.WithContext(entcache.NewContext(r.Context()))
        }
        next.ServeHTTP(w, r)
    })
})
```

A full runnable server example is located in [examples/ctxlevel](examples/ctxlevel).

---

### Driver-Level Cache

A driver-level cache stores cache entries on the `ent.Client`. Since an application typically creates one driver per database instance, this acts as a process-level cache shared across all application goroutines.

![driver-level-cache](https://github.com/ariga/entcache/blob/assets/internal/assets/drvlevel.png)

#### Create a default driver-level cache (unlimited LRU):

```go
drv := entcache.NewDriver(sqlDrv)
client := ent.NewClient(ent.Driver(drv))
```

#### Set TTL to 1 second:

```go
drv := entcache.NewDriver(sqlDrv, entcache.TTL(time.Second))
client := ent.NewClient(ent.Driver(drv))
```

#### Limit LRU size and set TTL:

```go
drv := entcache.NewDriver(
    sqlDrv,
    entcache.TTL(time.Second),
    entcache.Levels(entcache.NewLRU(128)),
)
client := ent.NewClient(ent.Driver(drv))
```

---

### Remote-Level Cache

Remote-level caching shares cached entries across multiple application instances. A remote cache layer is resistant to application deployments and restarts, reducing database load across distributed microservices.

#### Redis (go-redis)

```go
rdb := redis.NewClient(&redis.Options{Addr: ":6379"})
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(entcache.NewRedis(rdb)),
)
```

#### Rueidis (with Stampede Protection)

High-performance Redis client featuring stampede protection. When a cache miss occurs, only the first caller fetches from the database — concurrent callers wait on a channel until the cache entry is populated.

```go
c, err := rueidis.NewClient(rueidis.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
})
if err != nil {
    log.Fatal(err)
}
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(entcache.NewRueidis(c)),
)
```

#### NATS JetStream KV

Distributed cache backed by NATS JetStream KeyValue. Supports per-key TTL via `Create` and real-time invalidation notifications via `Watch`.

```go
nc, _ := nats.Connect(nats.DefaultURL)
js, _ := jetstream.New(nc)
kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
    Bucket: "entcache",
    MaxAge: 10 * time.Minute, // bucket-level TTL
})
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(entcache.NewNatsKV(kv)),
)
```

The `NatsKV` backend also exposes `Create` (SETNX equivalent) and `Watch` for building custom invalidation patterns:

```go
nkv := entcache.NewNatsKV(kv)

// Watch for changes — useful for cross-process local cache invalidation.
watcher, _ := nkv.Watch(ctx, ">") // watch all keys
go func() {
    for entry := range watcher.Updates() {
        if entry != nil {
            localCache.Del(ctx, entry.Key())
        }
    }
}()
```

---

### Multi-Level Cache

A cache hierarchy structures cache stores by access speed and capacity (e.g. L1 in-memory LRU + L2 remote Redis/NATS). Lookups cascade down the hierarchy: L1 → L2 → Database.

![multi-level-cache](https://github.com/ariga/entcache/blob/assets/internal/assets/multilevel.png)

```go
rdb := redis.NewClient(&redis.Options{
    Addr: ":6379",
})
drv := entcache.NewDriver(
    sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(
        entcache.NewLRU(256),   // Level 1: fast in-process memory
        entcache.NewRedis(rdb), // Level 2: durable shared Redis
    ),
)
client := ent.NewClient(ent.Driver(drv))
```

A full runnable server example is located in [examples/multilevel](examples/multilevel).

## Mutation-Aware Invalidation

`entcache` supports automatic cache invalidation on entity mutations (create, update, delete). A `ChangeSet` records modified entity keys and evicts stale cache entries on subsequent queries.

```go
// Create a ChangeSet with GC interval.
cs := entcache.NewChangeSet(5 * time.Minute)
cs.Start()
defer cs.Stop()

// Create the cached driver with ChangeSet.
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.WithKeyTTL(time.Hour),      // longer TTL for Get-by-ID queries
    entcache.WithChangeSet(cs),
)

// Create the client and register the mutation hook.
client := ent.NewClient(ent.Driver(drv))
client.Use(entcache.DataChangeNotify(drv))

// Now, when a User is updated:
client.User.UpdateOneID(42).SetName("new-name").Save(ctx)

// The next Get for that user will bypass the cache and re-fetch from DB.
u, _ := client.User.Get(entcache.WithEntryKey(ctx, "User", 42), 42)
```

### Dual TTL Strategy

Use short TTLs for hash-addressed queries (arbitrary SELECTs) and longer TTLs for key-addressed queries (Get-by-ID), since key-addressed queries are precisely invalidated by the mutation hook:

```go
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),           // hash queries: 1 minute TTL
    entcache.WithKeyTTL(time.Hour),      // key queries: 1 hour TTL (invalidated on mutation)
    entcache.WithChangeSet(cs),
)
```

## Per-Query Cache Control

Use context options to adjust caching behavior on individual queries:

```go
// Skip the cache entirely.
client.User.Query().All(entcache.Skip(ctx))

// Skip and invalidate the cache entry.
client.User.Query().All(entcache.Evict(ctx))

// Override TTL for a specific query.
client.User.Query().All(entcache.WithTTL(ctx, 30*time.Second))

// Use a custom cache key.
client.User.Query().All(entcache.WithKey(ctx, "my-custom-key"))

// Structured entry key for precise invalidation.
client.User.Get(entcache.WithEntryKey(ctx, "User", 42), 42)

// Don't cache empty results (e.g. entity not yet created).
client.User.Get(entcache.SkipNotFound(ctx), 42)
```

## Full Production Example (entpgx + Rueidis + Mutation Hook)

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/incroy/entcache"
    "github.com/incroy/entpgx"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/redis/rueidis"

    "myapp/ent"
)

func main() {
    ctx := context.Background()

    // 1. Database: native pgx pool via entpgx.
    pool, err := pgxpool.New(ctx, "postgres://localhost:5432/mydb")
    if err != nil {
        log.Fatal(err)
    }

    // 2. Remote Cache: rueidis with stampede protection.
    rc, err := rueidis.NewClient(rueidis.ClientOption{
        InitAddress: []string{"127.0.0.1:6379"},
    })
    if err != nil {
        log.Fatal(err)
    }

    // 3. ChangeSet for mutation-aware invalidation.
    cs := entcache.NewChangeSet(5 * time.Minute)
    cs.Start()
    defer cs.Stop()

    // 4. Wrap drivers: entpgx -> entcache -> ent.Client.
    pgxDrv := entpgx.NewDriver(pool)
    drv := entcache.NewDriver(pgxDrv,
        entcache.TTL(time.Minute),
        entcache.WithKeyTTL(30*time.Minute),
        entcache.WithChangeSet(cs),
        entcache.Levels(
            entcache.NewLRU(512),
            entcache.NewRueidis(rc),
        ),
    )
    client := ent.NewClient(ent.Driver(drv))

    // 5. Register the mutation hook.
    client.Use(entcache.DataChangeNotify(drv))

    // 6. Run migrations (skip cache).
    if err := client.Schema.Create(entcache.Skip(ctx)); err != nil {
        log.Fatal(err)
    }

    // Queries are cached across L1 memory and L2 Redis.
    // Mutations automatically invalidate stale keys!
}
```

## API Reference

Full documentation is available at [pkg.go.dev/github.com/incroy/entcache](https://pkg.go.dev/github.com/incroy/entcache).

### Option Functions

| Function | Description |
|---|---|
| `TTL(d)` | Default cache TTL for hash-addressed queries |
| `WithKeyTTL(d)` | Separate TTL for key-addressed (Get-by-ID) queries |
| `Hash(fn)` | Custom hash function for cache key generation |
| `Levels(...)` | Configure one or more cache backends |
| `ContextLevel()` | Use context-scoped caching |
| `WithChangeSet(cs)` | Enable mutation-aware invalidation |

### Context Helpers

| Function | Description |
|---|---|
| `Skip(ctx)` | Skip the cache for this query |
| `Evict(ctx)` | Skip and invalidate the cache entry |
| `WithTTL(ctx, d)` | Override TTL for this query |
| `WithKey(ctx, k)` | Use a custom cache key |
| `WithEntryKey(ctx, typ, id)` | Structured key for precise invalidation |
| `SkipNotFound(ctx)` | Don't cache empty results |
| `NewContext(ctx, ...)` | Attach a cache to the context (for ContextLevel) |

### Cache Backends

| Backend | Constructor | Use Case |
|---|---|---|
| LRU | `NewLRU(maxEntries)` | In-process, bounded cache |
| Redis (go-redis) | `NewRedis(client)` | Shared remote cache |
| Rueidis | `NewRueidis(client)` | High-perf Redis with stampede protection |
| NATS JetStream KV | `NewNatsKV(kv)` | Distributed cache with Watch notifications |
