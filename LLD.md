# LLD - Mini Redis

HLD me overall flow hai. Yahan classes aur thoda detail.

Folder roughly aisa hai:

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
Bas value + optional expiry time. Immutable rakha. Expiry check `isExpired(now)` se.

### LruCache
Yahi core hai.

- `LinkedHashMap` with accessOrder = true
- capacity cross -> eldest remove
- TTL lazy purge (get/set time pe check)
- lock: `ReentrantReadWriteLock`
  - get pe bhi write lock isliye kyuki access-order update hota hai. Read lock se race aa sakti thi.

Methods: `set`, `get`, `delete`, `snapshot`, `loadAll`, `keys`

Clock inject kiya hai taaki tests me time fake kar saku.

### SnapshotStore
Binary file. Format roughly:

```
MAGIC | VERSION | count
then for each key: key, value, expiryMillis (-1 if no ttl)
```

Pehle `.tmp` pe likhta hoon, phir rename. Windows pe atomic move fail ho to normal replace.

### AofLog
Text log. Lines jaise:
```
SET user ankit
SETEX session 3600000 token
DEL user
```

Space wale keys ke liye simple escape (`\s` etc). `rewrite()` live dataset se naya compact AOF banata hai.

### PersistenceManager
Glue. `recover()`, aur set/del pe AOF hooks. Snapshot alag se `saveSnapshot()`.

### PubSubHub
Map: channel -> list of listeners. Publish = sabko call. Cache se linked nahi.

### ReplCommand
Sealed: Set / SetEx / Del. Har command ke saath sequence number.

### ReplicationLeader
- followers list
- attach pe full sync (`cache.snapshot()`)
- har write pe sequence++ aur broadcast

### ReplicationFollower
- `fullSync` se start
- `apply` me sequence check (purana/duplicate ignore)
- khud se write ClusterNode pe block hai

### ConsistentHashRing
MD5 se hash. Har node ke liye kai virtual nodes ring pe. `route(key)` ceiling entry, wrap around to first.

### ClusterNode
Ek shard ka facade.

Roles:
- STANDALONE - akela node, persist allowed
- LEADER - writes + replicate
- FOLLOWER - read only

Factory methods: `standalone()`, `leader()`, `follower()`

set() roughly: cache update -> persistence hook -> replicate (agar leader)

### ClusterManager
Ring + shard list. `set/get/delete` locate karke forward. Followers ring pe nahi daalte (sirf leaders/standalone).

---

## Class relations (simple)

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

## SET flow (cluster + repl)

```
client.set(k,v)
  ClusterManager.locate(k) -> node
  node.set:
    cache.set
    aof.append (if on)
    leader.replicateSet -> each follower.apply
```

## Boot

```
new ClusterNode.leader(...)
  new LruCache
  new PersistenceManager
  recover:
    load rdb
    replay aof
```

---

## Thodi implementation notes

**LRU**  
Access order map + size check. Zyada kuch magic nahi.

**TTL**  
Absolute Instant store. Har access pe purge. Background expiry thread nahi hai abhi - lazy enough for this project.

**Replication gap**  
Follower lag / network partition handling nahi. In-process list pe broadcast hai.

**Hashing**  
Virtual nodes = 50 default cluster demo me. Kam nodes pe bina vnodes ke distribution gandi lagti thi isliye add kiya.

**Errors**
- blank key -> IllegalArgumentException
- write on follower -> IllegalStateException
- empty ring -> IllegalStateException
- bad rdb magic -> IOException

---

## Patterns - jo use hue / nahi hue

Jo use kiye:
- Facade -> ClusterNode, ClusterManager
- Factory methods -> node create
- Command-ish -> ReplCommand for repl stream

Jo force nahi kiye:
- Strategy for eviction (sirf LRU chahiye tha)
- Singleton (testing mushkil hoti)
- Decorator stack

Over-engineering avoid kiya. Interview me bhi yahi bolna better hai.

---

## Threading

| Jagah | Guard |
|---|---|
| LruCache map | RW lock |
| AOF writer | ReentrantLock + flush |
| PubSub | ConcurrentHashMap + CopyOnWriteArrayList |
| Followers list | CopyOnWriteArrayList |
| Hash ring | RW lock |
| repl sequence | AtomicLong |

---

## Kaise use karun (quick)

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

## Tests kya cover karte hain

- LRU eviction + TTL expiry
- snapshot save/load, AOF replay, recover order
- follower full sync + incremental
- hash ring same key -> same node, thoda even spread
- cluster set/get, follower write reject, pubsub deliver

`mvn test`  
demo: `mvn -q exec:java`

---

## Interview me commonly poochha jaata hai

**LRU kyun?**  
Simple, LinkedHashMap se natural fit. LFU ke liye frequency counters alag manage karne padte.

**RDB vs AOF?**  
RDB = fast load, beech ke writes lose ho sakte. AOF = har write log, recover better, file badi hoti hai isliye rewrite.

**Consistent hashing kyun?**  
Nodes change pe kam keys move. Modulo N pe reshuffle zyada.

**Follower pe write kyun band?**  
Split brain / conflict avoid. Single writer per shard.

**Kya improve karunga next?**  
RESP server, proper fsync policy, failover/election, slot based cluster, metrics (hit rate, repl lag).

---

Bas itna. Code me pehle `LruCache` kholna, phir baaki layers.
