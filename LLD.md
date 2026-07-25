# LLD - Mini Redis

HLD covers the overall flow. This file is about the classes and a few implementation details.

```
com.miniredis
  core/           CacheEntry, LruCache
  persistence/    SnapshotStore, AofLog, PersistenceManager
  pubsub/         PubSubHub
  replication/    ReplCommand, ReplicationLeader, ReplicationFollower
  sharding/       ConsistentHashRing
  cluster/        ClusterNode, ClusterManager
  Main.java
```

---

## Classes

### CacheEntry
Just a value + optional expiry. Immutable. Expiry check is `isExpired(now)`.

### LruCache
This is the core.

- `LinkedHashMap` with `accessOrder = true`
- over capacity -> remove eldest
- TTL is lazy (checked on get/set)
- locking via `ReentrantReadWriteLock`
  - `get` also takes the write lock because access-order updates the map. A read lock alone can race.

APIs: `set`, `get`, `delete`, `snapshot`, `loadAll`, `keys`

`Clock` is injected so tests can fake time.

### SnapshotStore
Binary file. Rough format:

```
MAGIC | VERSION | count
then for each entry: key, value, expiryMillis (-1 if no ttl)
```

Write to a `.tmp` file first, then rename. On Windows, atomic move can fail, so there is a fallback to a normal replace.

### AofLog
Text log. Lines look like:
```
SET user ankit
SETEX session 3600000 token
DEL user
```

Keys/values with spaces get a simple escape (`\s`, etc). `rewrite()` rebuilds a compact AOF from the live dataset.

### PersistenceManager
Glue layer. `recover()` plus hooks on set/delete for AOF. Snapshots are triggered with `saveSnapshot()`.

### PubSubHub
`channel -> list of listeners`. Publish calls everyone. Not tied to the KV store.

### ReplCommand
Sealed type: Set / SetEx / Del. Each command carries a sequence number.

### ReplicationLeader
- keeps follower list
- on attach: full sync from `cache.snapshot()`
- on each write: bump sequence and broadcast

### ReplicationFollower
- starts from `fullSync`
- `apply` ignores older/duplicate sequences
- local writes are blocked at `ClusterNode`

### ConsistentHashRing
MD5 hash. Multiple virtual nodes per physical node. `route(key)` takes the ceiling entry and wraps to the first if needed.

### ClusterNode
Facade for one shard.

Roles:
- STANDALONE - single node with persistence
- LEADER - accepts writes and replicates
- FOLLOWER - read only

Factories: `standalone()`, `leader()`, `follower()`

`set()` path: update cache -> persistence hook -> replicate (if leader)

### ClusterManager
Owns the ring + shard list. Forwards `set/get/delete` after locate. Only leaders/standalone nodes sit on the ring.

---

## How things connect

```
ClusterManager ----uses----> ConsistentHashRing
       |
       +---- owns ----> ClusterNode (many)
                            |
                            +--> LruCache
                            +--> PersistenceManager (optional)
                            +--> PubSubHub
                            +--> ReplicationLeader OR ReplicationFollower
```

---

## SET path (cluster + replication)

```
client.set(k, v)
  ClusterManager.locate(k) -> node
  node.set:
    cache.set
    aof.append (if enabled)
    leader.replicateSet -> follower.apply
```

## Boot path

```
ClusterNode.leader(...)
  create LruCache
  create PersistenceManager
  recover:
    load rdb
    replay aof
```

---

## Notes from implementation

**LRU**  
Access-order map + size check. Nothing fancy.

**TTL**  
Store an absolute `Instant`. Purge on access. No background sweeper yet - lazy expiry is enough for this project.

**Replication**  
No lag metrics, no network partitions. Just an in-process broadcast list.

**Hashing**  
Demo cluster uses 50 virtual nodes. Without vnodes, distribution looked ugly with only 3 shards.

**Errors**
- blank key -> IllegalArgumentException
- write on follower -> IllegalStateException
- empty ring -> IllegalStateException
- bad rdb magic -> IOException

---

## Patterns

Used:
- Facade - `ClusterNode`, `ClusterManager`
- Factory methods - node creation
- Command-style objects - `ReplCommand` for the repl stream

Skipped on purpose:
- Strategy for eviction (only LRU needed)
- Singleton (painful for tests)
- Decorator stacks

I did not want to force design patterns just to fill a diagram.

---

## Threading

| Place | Guard |
|---|---|
| LruCache map | RW lock |
| AOF writer | ReentrantLock + flush |
| PubSub | ConcurrentHashMap + CopyOnWriteArrayList |
| Followers list | CopyOnWriteArrayList |
| Hash ring | RW lock |
| repl sequence | AtomicLong |

---

## Quick usage

```java
// single node
var n = ClusterNode.standalone("n1", 1000, Path.of("data"), true);
n.set("k", "v");
n.set("s", "tok", Duration.ofMinutes(10));
n.saveSnapshot();

// leader + follower
var L = ClusterNode.leader("L", 1000, Path.of("data/L"), true);
var F = ClusterNode.follower("F", 1000);
L.asLeader().attach(F.asFollower());
L.set("k", "v");
F.get("k"); // v

// cluster
var c = new ClusterManager(50);
c.addShard(L);
c.set("user:1", "ankit");
```

---

## What the tests cover

- LRU eviction and TTL expiry
- snapshot save/load, AOF replay, recover order
- follower full sync + incremental updates
- hash ring: same key -> same node, roughly even spread
- cluster get/set, follower write reject, pubsub delivery

```bash
mvn test
mvn -q exec:java
```

---

## Questions I expect in interviews

**Why LRU?**  
Simple, and it maps cleanly to `LinkedHashMap`. LFU needs extra frequency tracking.

**RDB vs AOF?**  
RDB loads fast but you can lose writes since the last dump. AOF is more durable, grows faster, so you rewrite it.

**Why consistent hashing?**  
Fewer keys move when nodes change. Modulo-N reshuffles almost the whole keyspace.

**Why block writes on followers?**  
Keeps a single writer per shard. Avoids conflicts.

**What would I add next?**  
A RESP server, better fsync policy, leader election/failover, slot-based clustering, and basic metrics (hit rate, repl lag).

---

Start with `LruCache`, then walk outward through the layers.
