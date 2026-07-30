# entcache

[![Go Reference](https://pkg.go.dev/badge/github.com/incroy/entcache.svg)](https://pkg.go.dev/github.com/incroy/entcache)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A production-ready, modular cache driver for [ent](https://github.com/ent/ent) with built-in distributed stampede protection, real-time invalidation event streaming, and modular sub-packages:

- **Modular Storage Backends** — clean sub-packages for `contextcache` (per-request in-memory cache with stampede protection), `lrucache` (Hashicorp LRU v2), `natscache` (NATS JetStream KV), `rediscache` (go-redis), and `rueidiscache` (Rueidis with native RESP3 client-side caching). Zero bloated dependencies in core `entcache`.
- **Automatic Stampede Protection** — built-in distributed locking (`KV.Create` placeholder lock + `Watch` waiting) for NATS, channel lock waiting for Rueidis, and singleflight deduplication for LRU/Redis.
- **Native Client-Side Caching** — `rueidiscache` leverages Rueidis RESP3 client-side caching out of the box (in-memory client cache with server-driven invalidation) with no extra LRU required.
- **Context-level** — per-request cache attached to a `context.Context` (e.g. HTTP request or GraphQL resolve) that eliminates duplicate queries within a single request.
- **Driver-level** — process-level cache embedded in the `ent.Client` and shared across all goroutines.
- **Multi-level** — hierarchical cache structure (e.g. L1 LRU memory cache + L2 remote Redis/NATS store) for optimal latency and durability.
- **Mutation-aware invalidation** — ent hooks automatically invalidate stale cache entries when entity mutations (create, update, delete) occur.

Compatible with standard `database/sql` drivers as well as native drivers like [entpgx](https://github.com/incroy/entpgx).

## Installation

Install core `entcache` along with your preferred cache backend sub-package:

```shell
# Core entcache package
go get github.com/incroy/entcache

# Optional modular backends
go get github.com/incroy/entcache/contextcache # Request-scoped in-memory cache
go get github.com/incroy/entcache/lrucache      # Hashicorp LRU v2
go get github.com/incroy/entcache/natscache     # NATS JetStream KV
go get github.com/incroy/entcache/rediscache    # go-redis/v9
go get github.com/incroy/entcache/rueidiscache  # Rueidis (Native RESP3 Client-Side Caching)
```

## Quick Start

### With `database/sql` Driver & Hashicorp LRU

```go
import (
    "github.com/incroy/entcache"
    "github.com/incroy/entcache/lrucache"
)

// Open the database connection.
db, err := sql.Open(dialect.Postgres, "postgres://localhost:5432/mydb?sslmode=disable")
if err != nil {
    log.Fatal("opening database", err)
}

// Wrap the sql.Driver with entcache.Driver.
drv := entcache.NewDriver(
    sql.OpenDB(dialect.Postgres, db),
    entcache.TTL(time.Minute),
    entcache.Levels(lrucache.MustNew(1000)),
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
    "github.com/incroy/entcache/lrucache"
)

// Create a pgxpool.Pool.
pool, err := pgxpool.New(ctx, "postgres://localhost:5432/mydb?sslmode=disable")
if err != nil {
    log.Fatal(err)
}

// Create the entpgx driver.
pgxDrv := entpgx.NewDriver(pool)

// Wrap with entcache.
drv := entcache.NewDriver(
    pgxDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(lrucache.MustNew(1024)),
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

## Modular Cache Backends

### 1. Hashicorp LRU (`lrucache`)

Thread-safe in-process LRU cache powered by `github.com/hashicorp/golang-lru/v2`. Fast, zero-network-overhead.

```go
import "github.com/incroy/entcache/lrucache"

drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(lrucache.MustNew(1000)),
)
```

### 2. NATS JetStream KV (`natscache`)

Distributed cache backed by NATS JetStream KeyValue (`github.com/incroy/entcache/natscache`).

**Out-of-the-Box Features**:
- **Distributed Stampede Protection**: On a cache miss, the winner acquires a `KV.Create` placeholder lock and executes the DB query. Concurrent callers on other nodes automatically wait via `KV.Watch` until the value is populated.
- **Real-Time Cross-Node Invalidation**: Listens to NATS `Watch` events (`KeyValueDelete` / `KeyValuePurge`) and automatically evicts stale keys across all application nodes.

```go
import (
    "github.com/nats-io/nats.go/jetstream"
    "github.com/incroy/entcache/natscache"
)

nc, _ := nats.Connect(nats.DefaultURL)
js, _ := jetstream.New(nc)
kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
    Bucket: "entcache",
    MaxAge: 10 * time.Minute,
})

drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(natscache.New(kv)),
)
```

### 3. Rueidis (`rueidiscache`)

High-performance Redis cache powered by Rueidis (`github.com/incroy/entcache/rueidiscache`).

**Out-of-the-Box Features**:
- **Native RESP3 Client-Side Caching**: Natively activated out of the box! Rueidis caches keys in-memory on the client and receives server-driven invalidation tracking messages from Redis. No extra L1 LRU layer is required.
- **Stampede Protection**: Built-in channel wait locks prevent multiple concurrent database queries for the same key.

```go
import (
    "github.com/redis/rueidis"
    "github.com/incroy/entcache/rueidiscache"
)

c, err := rueidis.NewClient(rueidis.ClientOption{
    InitAddress: []string{"127.0.0.1:6379"},
})
if err != nil {
    log.Fatal(err)
}

// Native Client-Side Caching is active out of the box!
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(rueidiscache.New(c)),
)
```

### 4. Redis (`rediscache`)

Standard remote cache backed by `github.com/redis/go-redis/v9` (`github.com/incroy/entcache/rediscache`).

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/incroy/entcache/rediscache"
)

rdb := redis.NewClient(&redis.Options{Addr: ":6379"})
drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(rediscache.New(rdb)),
)
```

### 5. Context Cache (`contextcache`)

Per-request memory cache (`github.com/incroy/entcache/contextcache`) with built-in channel lock waiting for stampede protection across goroutines handling a request context.

```go
import "github.com/incroy/entcache/contextcache"

drv := entcache.NewDriver(sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(contextcache.New()),
)
```

---

## Caching Levels

`entcache` provides several builtin cache levels:

1. **`context.Context` Cache** — Attached to a request (e.g. HTTP request or GraphQL resolve). Used to eliminate duplicate database queries executed during the same request lifecycle.
2. **Driver-Level Cache** — Embedded in `ent.Client`. Shared across all goroutines in the application process.
3. **Remote-Level Cache** — Remote cache (NATS KV, Rueidis, Redis) providing persistence and sharing cache entries across multiple service replicas.
4. **Multi-Level Cache** — Hierarchical cache structure combining fast in-memory LRU caching with remote persistent backends.

---

### Context-Level Cache

Scoped to a single `context.Context` (e.g. `*http.Request`). The context carries an in-memory cache to eliminate duplicate database queries executed during the same request lifecycle.

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

Ent optimizes this by batching execution into 3 queries. With `entcache`, the number of queries is further reduced from 3 to **2**, because query 1 (fetch users) and query 3 (fetch todo owners) execute identical SQL statements, allowing query 3 to be served directly from context cache.

![context-level-cache](https://github.com/ariga/entcache/blob/assets/internal/assets/ctxlevel.png)

#### Usage In GraphQL

```go
drv := entcache.NewDriver(sqlDrv, entcache.ContextLevel())
client := ent.NewClient(ent.Driver(drv))
```

Wrap the request `context.Context` with `entcache.NewContext` when a GraphQL query arrives:

```go
srv.AroundResponses(func(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
    if op := graphql.GetOperationContext(ctx).Operation; op != nil && op.Operation == ast.Query {
        ctx = entcache.NewContext(ctx)
    }
    return next(ctx)
})
```

A full runnable server example is located in [examples/ctxlevel](examples/ctxlevel).

---

### Multi-Level Cache

A cache hierarchy structures cache stores by access speed and capacity (e.g. L1 in-memory LRU + L2 remote NATS KV/Redis). Lookups cascade down the hierarchy: L1 → L2 → Database.

![multi-level-cache](https://github.com/ariga/entcache/blob/assets/internal/assets/multilevel.png)

```go
drv := entcache.NewDriver(
    sqlDrv,
    entcache.TTL(time.Minute),
    entcache.Levels(
        lrucache.MustNew(256),  // Level 1: fast in-process memory
        natscache.New(kv),      // Level 2: durable NATS JetStream KV
    ),
)
client := ent.NewClient(ent.Driver(drv))
```

A full runnable server example is located in [examples/multilevel](examples/multilevel).

---

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

---

## Full Production Example (entpgx + NATS KV + Mutation Hook)

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/incroy/entcache"
    "github.com/incroy/entcache/lrucache"
    "github.com/incroy/entcache/natscache"
    "github.com/incroy/entpgx"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/nats-io/nats.go"
    "github.com/nats-io/nats.go/jetstream"

    "myapp/ent"
)

func main() {
    ctx := context.Background()

    // 1. Database: native pgx pool via entpgx.
    pool, err := pgxpool.New(ctx, "postgres://localhost:5432/mydb")
    if err != nil {
        log.Fatal(err)
    }

    // 2. Remote Cache: NATS JetStream KV.
    nc, err := nats.Connect(nats.DefaultURL)
    if err != nil {
        log.Fatal(err)
    }
    js, err := jetstream.New(nc)
    if err != nil {
        log.Fatal(err)
    }
    kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
        Bucket: "entcache",
        MaxAge: 10 * time.Minute,
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
            lrucache.MustNew(512),
            natscache.New(kv),
        ),
    )
    client := ent.NewClient(ent.Driver(drv))

    // 5. Register the mutation hook.
    client.Use(entcache.DataChangeNotify(drv))

    // 6. Run migrations (skip cache).
    if err := client.Schema.Create(entcache.Skip(ctx)); err != nil {
        log.Fatal(err)
    }

    // Queries are cached across L1 memory and L2 NATS KV.
    // Distributed stampede locking & real-time Watch invalidation work out of the box!
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

### Context Options (Per-Query Control)

| Function | Description |
|---|---|
| `Skip(ctx)` | Skip cache for a query |
| `Evict(ctx)` | Skip and invalidate cache entry for a query |
| `WithKey(ctx, key)` | Explicitly set cache key for a query |
| `WithTTL(ctx, ttl)` | Custom TTL for a query |
| `WithEntryKey(ctx, typ, id)` | Structured entity key (e.g. `"User:42"`) for Get-by-ID queries & ChangeSet invalidation |
| `SkipNotFound(ctx)` | Prevent caching when query result contains 0 rows |

### Cache Backends

| Sub-Package | Package Name | Constructor | Features |
|---|---|---|---|
| `github.com/incroy/entcache/contextcache` | `contextcache` | `contextcache.New()` | Per-request cache with channel stampede locking |
| `github.com/incroy/entcache/lrucache` | `lrucache` | `lrucache.New(size)` | Hashicorp LRU v2 in-process cache |
| `github.com/incroy/entcache/natscache` | `natscache` | `natscache.New(kv)` | NATS KV Create stampede lock & Watch invalidation |
| `github.com/incroy/entcache/rueidiscache` | `rueidiscache` | `rueidiscache.New(client)` | Native RESP3 Client-Side Caching & lock channels |
| `github.com/incroy/entcache/rediscache` | `rediscache` | `rediscache.New(client)` | standard `go-redis/v9` backend |
