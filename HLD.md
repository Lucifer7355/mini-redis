# HLD - Mini Redis

I built this to understand how Redis-like systems work internally: LRU, TTL, RDB/AOF, replication, and sharding. This is not a production Redis. Nodes run in the same JVM so the concepts stay easy to demo and debug.

---

## What I was solving

I wanted a small distributed cache that can:

- do fast get/set in memory
- evict old keys when full (LRU)
- expire keys after a TTL
- survive restarts without losing everything (persistence)
- keep a read replica (leader / follower)
- split keys across nodes (sharding)
- push messages on channels (pubsub)

---

## Big picture

The client talks to `ClusterManager`. The manager hashes the key and forwards the call to the right shard.

Each shard is a `ClusterNode`. Inside it:

- `LruCache` holds the data
- persistence writes to disk
- pubsub is available on the node
- if the node is a leader, it replicates to followers

```
Client
  -> ClusterManager (consistent hash)
       -> shard-1 (leader) ---> follower-1
       -> shard-2 (leader)
       -> shard-3 (leader)
```

Followers do not accept writes. They only serve reads and apply updates from the leader.

---

## Components

**LruCache**  
The actual store. `LinkedHashMap` in access-order. When capacity is crossed, the least recently used key goes out. TTL checks live here too.

**PersistenceManager**  
Two pieces:
1. Snapshot / RDB-style file (`dump.rdb`) - full dump
2. AOF (`appendonly.aof`) - one line per write

On restart: load snapshot first, then replay AOF.

**ReplicationLeader / Follower**  
Writes hit the leader, then get pushed to followers. A new follower gets a full snapshot on attach, then incremental SET / SETEX / DEL.

**ConsistentHashRing**  
Maps key -> node. Uses virtual nodes so distribution is less skewed. I avoided plain `hash % N` because adding/removing a node reshuffles almost everything.

**ClusterManager**  
Top-level API. Routes `set` / `get` / `delete` to the owning node.

**PubSubHub**  
Separate from the KV store. Subscribe / publish only. Messages are not saved as cache entries.

---

## Main flows

### Write
1. ClusterManager finds the shard for the key
2. Leader updates its cache
3. If AOF is on, append to disk
4. Replicate to followers

### Read
Locate shard -> `cache.get`. Miss if the key expired or got evicted.

### Restart
Load `dump.rdb` -> replay `appendonly.aof` -> ready

### Follower attach
Leader sends current data + sequence number, then continues with the normal replication stream.

---

## Tradeoffs I knowingly made

- AOF flushes on every write. Safer, slower. Real Redis has options like `everysec`.
- Replication is fire-and-forget here (in-process). No follower ack / lag tracking.
- No live resharding. New shards get new keys; old keys do not migrate automatically.
- No network protocol (RESP). I kept the focus on internals.

---

## How I think about scaling

| Need | What to do |
|---|---|
| More memory / write throughput | add shards |
| More reads | add followers |
| Less data loss | keep AOF + take snapshots often |
| Failover | not automated yet - no leader election |

---

## Out of scope

- Redis wire protocol
- Auto leader election
- Cross-shard transactions
- Live key migration

If someone asks what is missing for production, this is the honest list.

---

Suggested reading order: `LruCache` -> persistence -> replication -> hash ring -> `ClusterNode` -> `ClusterManager` -> `Main`.

Class-level detail: [LLD.md](./LLD.md)
