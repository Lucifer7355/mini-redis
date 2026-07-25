# Low Level Design (LLD) — Mini Redis

> Goal: classes, APIs, concurrency, patterns — code kholne se pehle design clear ho.

Related: [`HLD.md`](./HLD.md)

---

## 1. Package structure

```text
com.miniredis
├── Main.java                 # demo / walkthrough
├── core/
│   ├── CacheEntry            # value + optional expiry
│   └── LruCache              # thread-safe LRU + TTL store
├── persistence/
│   ├── SnapshotStore         # RDB-style binary dump
│   ├── AofLog                # append-only command log
│   └── PersistenceManager    # recover / hook writes
├── pubsub/
│   └── PubSubHub             # channel → listeners
├── replication/
│   ├── ReplCommand           # sealed SET | SETEX | DEL
│   ├── ReplicationLeader
│   └── ReplicationFollower
├── sharding/
│   └── ConsistentHashRing<T>
└── cluster/
    ├── ClusterNode           # one shard (role-aware facade)
    └── ClusterManager        # multi-shard router
```

---

## 2. Core entities

### `CacheEntry`
| | |
|---|---|
| **Responsibility** | Immutable value + optional `expiresAt` |
| **Ownership** | Owned by `LruCache` |
| **Lifecycle** | Created on SET; discarded on DEL / eviction / expiry |

### `LruCache`
| | |
|---|---|
| **Responsibility** | Capacity-bounded map; LRU eviction; lazy TTL purge |
| **Associations** | 1 → many `CacheEntry` |
| **Concurrency** | `ReentrantReadWriteLock` (write lock also on `get` because access-order updates) |
| **Key APIs** | `set`, `get`, `delete`, `snapshot`, `loadAll`, `keys` |

### `PersistenceManager`
| | |
|---|---|
| **Responsibility** | Orchestrate snapshot + AOF around cache mutations |
| **Composition** | Owns `SnapshotStore` + optional `AofLog` |
| **Recovery** | `load snapshot` → `replay AOF` |

### `PubSubHub`
| | |
|---|---|
| **Responsibility** | Subscribe / unsubscribe / publish |
| **Storage** | `ConcurrentHashMap<channel, CopyOnWriteArrayList<listener>>` |
| **Note** | Messages are **not** persisted in the KV store |

### `ReplicationLeader` / `ReplicationFollower`
| | |
|---|---|
| **Leader** | Sequence counter; attach → full sync; broadcast commands |
| **Follower** | Apply commands if `seq > applied`; rejects local writes via `ClusterNode` |
| **Cardinality** | 1 Leader → 0..N Followers |

### `ConsistentHashRing<T>`
| | |
|---|---|
| **Responsibility** | Map key → node using MD5 + virtual nodes |
| **Why vnodes** | Better balance when node count is small |

### `ClusterNode`
| | |
|---|---|
| **Responsibility** | Facade for one shard: cache + persist + pubsub + repl role |
| **Roles** | `STANDALONE`, `LEADER`, `FOLLOWER` |
| **Factory** | `standalone()`, `leader()`, `follower()` |

### `ClusterManager`
| | |
|---|---|
| **Responsibility** | Add/remove shards; route SET/GET/DEL |
| **Association** | Aggregates leader/standalone nodes on the ring |

---

## 3. Class diagram (UML)

```text
┌──────────────────┐          routes           ┌──────────────────┐
│ ClusterManager   │──────────────────────────▶│ ConsistentHashRing│
│  + set/get/del   │                           │  + add/remove/route│
└────────┬─────────┘                           └──────────────────┘
         │ 1..*
         ▼
┌──────────────────────────────────────────────┐
│                 ClusterNode                  │
│  role: STANDALONE | LEADER | FOLLOWER        │
│  + set/get/delete/saveSnapshot/rewriteAof    │
└───┬──────────┬──────────┬──────────┬─────────┘
    │1         │0..1      │1         │0..1
    ▼          ▼          ▼          ▼
 LruCache   Persistence  PubSubHub  ReplicationLeader
               │                         │
               │                    broadcasts
        SnapshotStore                    ▼
        AofLog              ReplicationFollower ──▶ LruCache
                                    ▲
                                    │ applies
                               ReplCommand
                            (Set|SetEx|Del)
```

---

## 4. Sequence diagrams

### SET with TTL on a cluster leader

```text
Client          ClusterManager       ClusterNode(L)     LruCache     Persistence     ReplicationLeader     Follower
  │                   │                    │               │              │                  │                 │
  │ set(k,v,ttl)      │                    │               │              │                  │                 │
  │──────────────────▶│ locate(k)          │               │              │                  │                 │
  │                   │───────────────────▶│ set(...)      │              │                  │                 │
  │                   │                    │──────────────▶│              │                  │                 │
  │                   │                    │               │ put+evict    │                  │                 │
  │                   │                    │──────────────▶│ onSetEx      │                  │                 │
  │                   │                    │               │─────────────▶│ AOF append       │                 │
  │                   │                    │───────────────────────────────────────────────▶│ replicateSetEx  │
  │                   │                    │               │              │                  │────────────────▶│ apply
```

### Startup recovery

```text
ClusterNode.standalone / leader
        │
        ▼
PersistenceManager.recover()
        │
        ├── SnapshotStore.load(cache)     // dump.rdb
        └── AofLog.replay(cache)          // appendonly.aof
```

---

## 5. Important algorithms

### LRU eviction
1. `LinkedHashMap(capacity, load, accessOrder=true)`
2. On `get` / `put`, entry moves to most-recent end
3. While `size > capacity`, remove eldest (iterator first key)

### TTL
- Store absolute `Instant expiresAt`
- On read/write: purge expired entries (lazy)
- Expired key ≡ miss

### Consistent hashing
1. For each physical node, insert `V` virtual nodes: `hash(nodeId + "#" + i)`
2. Key hash → clockwise next vnode on sorted ring
3. Empty ring → error

### Replication sequencing
- Leader `AtomicLong` sequence
- Follower ignores `command.seq <= appliedSeq` (idempotent / late dup safe)

### Snapshot format
```text
MAGIC(int) | VERSION(int) | COUNT(int)
  repeated: KEY(utf) | VALUE(utf) | EXPIRY_EPOCH_MS(long, -1 if none)
```
Write to `.tmp` then rename (crash-safer).

### AOF lines
```text
SET key value
SETEX key ttlMillis value
DEL key
```
Spaces in keys/values escaped (`\s`, `\n`, `\\`).

---

## 6. Pattern evaluation (used vs not)

| Pattern | Used? | Reason |
|---|---|---|
| **Facade** | YES | `ClusterNode` / `ClusterManager` simplify subsystems |
| **Factory method** | YES | `ClusterNode.leader/follower/standalone` |
| **Strategy** | NO | Single eviction (LRU) / single hash — no runtime switch needed |
| **Observer** | YES (light) | Pub/Sub listeners; repl followers are push targets |
| **Command** | YES | `ReplCommand` sealed hierarchy for replication stream |
| **Singleton** | NO | Nodes are explicitly constructed (testable) |
| **Decorator** | NO | No cross-cutting wrapper stack |
| **Template Method** | NO | Recovery flow is linear, not subclassed |
| **Repository** | NO | Cache itself is the store |

---

## 7. SOLID mapping

| Principle | Where |
|---|---|
| **S** | `LruCache` ≠ persistence ≠ pubsub ≠ routing |
| **O** | New `ReplCommand` variants via sealed permits |
| **L** | Roles enforced by API (`follower.set` throws) rather than fake overrides |
| **I** | Small focused types (`SnapshotStore` vs `AofLog`) |
| **D** | `Clock` injected into `LruCache` (tests control time) |

---

## 8. Thread safety

| Shared state | Guard |
|---|---|
| `LruCache.store` | `ReentrantReadWriteLock` |
| `AofLog.writer` | `ReentrantLock` + flush |
| `PubSubHub.channels` | `ConcurrentHashMap` + COW lists |
| `ReplicationLeader.followers` | `CopyOnWriteArrayList` |
| `ConsistentHashRing.ring` | RW lock |
| Leader sequence | `AtomicLong` |

**Race to avoid:** updating LinkedHashMap access order under only a read lock — isliye `get` write-lock leta hai.

---

## 9. Public API cheat-sheet

```java
// Single node
ClusterNode n = ClusterNode.standalone("n1", 10_000, Path.of("data"), true);
n.set("k", "v");
n.set("session", "tok", Duration.ofMinutes(30));
n.get("k");
n.saveSnapshot();
n.rewriteAof();

// Replication
ClusterNode L = ClusterNode.leader("L", 10_000, Path.of("data/L"), true);
ClusterNode F = ClusterNode.follower("F", 10_000);
L.asLeader().attach(F.asFollower());
L.set("k", "v");          // replicated
F.get("k");               // read replica
// F.set(...) → IllegalStateException

// Cluster
ClusterManager c = new ClusterManager(50);
c.addShard(L);
c.set("user:1", "ankit"); // hashed to a shard
c.get("user:1");

// Pub/Sub
n.pubSub().subscribe("news", (ch, msg) -> ...);
n.pubSub().publish("news", "hello");
```

---

## 10. Error / validation rules

| Case | Behavior |
|---|---|
| Blank key / channel | `IllegalArgumentException` |
| Capacity ≤ 0 | `IllegalArgumentException` |
| Write on follower | `IllegalStateException` |
| Empty hash ring | `IllegalStateException` |
| Bad snapshot magic/version | `IOException` |
| Unknown AOF command | `IllegalStateException` |

---

## 11. Test map

| Area | Tests |
|---|---|
| LRU / TTL | `LruCacheTest` |
| Snapshot / AOF / recover | `PersistenceTest` |
| Full sync + incremental | `ReplicationTest` |
| Ring stability / balance | `ConsistentHashRingTest` |
| Cluster + follower reject + pubsub | `ClusterIntegrationTest` |

Run: `mvn test`  
Demo: `mvn -q exec:java`

---

## 12. Interview Q&A (short)

**Q: LRU vs LFU?**  
A: LRU “recently unused” hataata hai — simple + LinkedHashMap-friendly. LFU frequency track karega (extra metadata).

**Q: Snapshot vs AOF?**  
A: Snapshot = fast load, bigger loss window. AOF = finer durability, slower writes / larger log → rewrite se compact.

**Q: Why consistent hashing?**  
A: Node add/remove pe sirf nearby keys move hote hain (ideal case). Modulo-N pe almost sab keys reshuffle.

**Q: Sync vs async replication?**  
A: Yahan in-process async-style broadcast. Real system mein sync = less data loss, higher write latency.

**Q: CAP?**  
A: Per-shard single leader → CP-leaning on that shard’s writes. Partition + multi-leader nahi hai.

---

## 13. Future extensions (agar aur deep jaana ho)

1. TCP server + Redis-compatible RESP  
2. Raft / election for auto failover  
3. Slot-based cluster (Redis Cluster style 16384 slots)  
4. Async AOF everysec fsync policy  
5. Metrics: hit ratio, eviction count, repl lag  

---

## 14. Mental model (30 seconds)

```text
ClusterManager  =  traffic police (which shard?)
ClusterNode     =  one Redis-like process
LruCache        =  memory
Persistence     =  disk insurance
Replication     =  copy to standby
PubSub          =  live notifications (alag se)
```

Isi order mein code padho: `LruCache` → `Persistence*` → `Replication*` → `ConsistentHashRing` → `ClusterNode` → `ClusterManager` → `Main`.
