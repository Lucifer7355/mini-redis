# High Level Design (HLD) — Mini Redis

> Goal: interview / resume pe clearly explain kar sako — *ye project kya hai, kahan pe kya fit hota hai, data kaise flow karta hai*.

---

## 1. Problem statement

Redis jaisa **distributed in-memory cache** banana hai jo:

- fast key-value get/set de
- memory bound ho (LRU)
- keys expire ho sake (TTL)
- restart ke baad data wapas aa sake (persistence)
- ek node fail hone pe read continue ho (replication)
- data multiple machines pe split ho (sharding / cluster)
- realtime messaging support kare (pub/sub)

Ye production Redis nahi hai — **learning + system-design showcase** project hai (in-process / same-JVM cluster simulation).

---

## 2. System context

```text
                 ┌──────────────────────────────┐
   Client / Demo │         ClusterManager       │
   (Main / API)  │   (consistent-hash router)   │
                 └──────────────┬───────────────┘
                                │ route(key)
          ┌─────────────────────┼─────────────────────┐
          ▼                     ▼                     ▼
   ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
   │ Shard-1     │       │ Shard-2     │       │ Shard-3     │
   │ (LEADER)    │       │ (LEADER)    │       │ (LEADER)    │
   │  LruCache   │       │  LruCache   │       │  LruCache   │
   │  Persist    │       │  Persist    │       │  Persist    │
   │  PubSub     │       │  PubSub     │       │  PubSub     │
   └──────┬──────┘       └─────────────┘       └─────────────┘
          │ replicate
          ▼
   ┌─────────────┐
   │ Follower(s) │  read-only replica
   └─────────────┘
```

**Actors**

| Actor | Role |
|---|---|
| Application / `Main` | Commands bhejta hai (SET/GET/DEL/PUBLISH…) |
| `ClusterManager` | Key → sahi shard decide karta hai |
| `ClusterNode` (Leader) | Writes accept, persist, replicate |
| `ClusterNode` (Follower) | Reads only, leader se sync |
| Disk (`dump.rdb`, `appendonly.aof`) | Durability |

---

## 3. Functional requirements

| ID | Requirement |
|---|---|
| FR1 | SET / GET / DELETE key-value |
| FR2 | Capacity exceed → **LRU eviction** |
| FR3 | Optional **TTL**; expire ke baad key invisible |
| FR4 | **Snapshot (RDB-style)** dump + load |
| FR5 | **AOF** append log + replay + rewrite |
| FR6 | **Pub/Sub** channels |
| FR7 | **Leader → Follower** replication (full sync + incremental) |
| FR8 | Followers **read-only** |
| FR9 | **Consistent hashing** se sharding |
| FR10 | **Cluster** multi-shard routing |

---

## 4. Non-functional requirements

| Area | Choice in this project |
|---|---|
| Latency | In-memory `LinkedHashMap`; O(1) avg get/set |
| Consistency | Single leader per shard (strong on leader); async repl to followers |
| Durability | Snapshot + AOF (AOF every write flush) |
| Scalability | Horizontal via more shards on hash ring |
| Availability | Follower se reads (failover automation *not* implemented) |
| Thread safety | `ReadWriteLock` / concurrent collections on shared state |

---

## 5. High-level components

```text
┌─────────────────────────────────────────────────────────────┐
│                        CLUSTER LAYER                        │
│  ClusterManager  →  ConsistentHashRing  →  ClusterNode[]    │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────┴───────────────────────────────┐
│                         NODE LAYER                          │
│  Role: STANDALONE | LEADER | FOLLOWER                       │
│  ┌──────────┐ ┌────────────┐ ┌────────┐ ┌────────────────┐  │
│  │ LruCache │ │Persistence │ │ PubSub │ │ Replication*   │  │
│  │ + TTL    │ │ Snapshot   │ │  Hub   │ │ Leader/Follow  │  │
│  │          │ │ AOF        │ │        │ │                │  │
│  └──────────┘ └────────────┘ └────────┘ └────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

| Component | Responsibility |
|---|---|
| **LruCache** | Hot data; LRU + TTL |
| **PersistenceManager** | Snapshot save/load + AOF append/replay |
| **PubSubHub** | Channel fan-out messaging |
| **ReplicationLeader / Follower** | Copy writes to replicas |
| **ConsistentHashRing** | Key → shard mapping |
| **ClusterManager** | Cluster-wide SET/GET/DEL API |

---

## 6. End-to-end flows

### 6.1 Write path (cluster)

```text
Client SET key=user:1 value=ankit
        │
        ▼
ClusterManager.locate(key)  ──hash──►  Shard-2 (LEADER)
        │
        ▼
Leader.cache.set(...)
        │
        ├──► AOF append  (if enabled)
        └──► ReplicationLeader.broadcast(SET) ──► Followers.apply()
```

### 6.2 Read path

```text
Client GET key
   → ClusterManager.locate(key)
   → owning shard LruCache.get
   → miss if expired / evicted / never set
```

Follower se direct read bhi possible (same key’s replica), lekin cluster router by default **leader shards** pe jaata hai.

### 6.3 Crash recovery

```text
Node start
  1. Load dump.rdb   (point-in-time snapshot)
  2. Replay appendonly.aof  (writes after / including log)
  3. Serve traffic
```

### 6.4 Replication attach

```text
Follower attaches to Leader
  1. Leader sends full cache snapshot + sequence #
  2. Follower loadAll(snapshot)
  3. Later writes → ReplCommand stream (SET / SETEX / DEL)
```

### 6.5 Pub/Sub

```text
SUBSCRIBE news  →  register listener on channel
PUBLISH news m  →  fan-out to all listeners (not stored in cache)
```

---

## 7. Data model (logical)

| Concept | Fields |
|---|---|
| Key | non-blank string |
| Value | string |
| Expiry | optional absolute timestamp |
| Shard | nodeId owning the key |
| Repl seq | monotonic long on leader |

Physical files:

- `dump.rdb` — binary snapshot  
- `appendonly.aof` — text command log (`SET`, `SETEX`, `DEL`)

---

## 8. Capacity & scaling story (interview)

| Scale | Approach |
|---|---|
| Single machine | One `STANDALONE` / `LEADER` node |
| More memory / QPS | Add shards → ring rebalance (keys remapped; *live migration not built*) |
| Read scale | Attach followers per shard |
| Durability tradeoff | Snapshot only = faster, more data loss window; AOF = safer, more disk I/O |

---

## 9. What is **not** in scope (honest)

- Network protocol / Redis RESP wire format  
- Automatic leader election / failover  
- Cross-shard transactions  
- Live resharding / key migration  
- Disk-backed values beyond snapshot/AOF  

Yeh intentionally rakha hai taaki core ideas clear rahein.

---

## 10. One-line pitch (resume / interview)

> “I built a Mini Redis: in-memory LRU+TTL cache with RDB/AOF persistence, leader-follower replication, consistent-hash sharding, and pub/sub — implemented in Java 21 with unit tests and an end-to-end demo.”

Next: detailed class design → [`LLD.md`](./LLD.md)
