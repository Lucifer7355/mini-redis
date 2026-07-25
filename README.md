# Mini Redis — Distributed Cache

A from-scratch Redis-inspired cache in **Java 21**. Built to practice (and demo) the core ideas behind an in-memory distributed store.

## Features

| Feature | What it does |
|---|---|
| **LRU** | Capacity-bounded cache; least recently used keys get evicted |
| **TTL** | Per-key expiry; lazy purge on access |
| **Snapshot (RDB)** | Point-in-time binary dump (`dump.rdb`) |
| **AOF** | Append-only command log + rewrite/compaction |
| **Pub/Sub** | Channel subscribe / publish fan-out |
| **Replication** | Leader → follower full sync + incremental stream |
| **Leader / Follower** | Writes on leader only; followers are read-only |
| **Sharding** | Consistent hashing with virtual nodes |
| **Cluster** | Multi-shard manager that routes keys to the right node |

## Project layout

```
src/main/java/com/miniredis/
  core/           LRU + TTL cache
  persistence/    Snapshot + AOF
  pubsub/         Pub/Sub hub
  replication/    Leader / follower
  sharding/       Consistent hash ring
  cluster/        Node + cluster manager
  Main.java       End-to-end demo
```

## Run

```bash
mvn test
mvn -q exec:java
```

## Design docs

- **[HLD.md](./HLD.md)** — system context, components, flows, scaling story  
- **[LLD.md](./LLD.md)** — classes, UML, sequences, concurrency, patterns, interview Q&A  

## Design notes

- **LRU**: `LinkedHashMap` in access-order + write lock on mutate/get (get must update order).
- **Persistence**: on boot → load snapshot, then replay AOF. Snapshot is crash-friendly (write temp + rename).
- **Replication**: follower does a snapshot full-sync on attach, then applies `SET` / `SETEX` / `DEL` with monotonic sequence numbers.
- **Cluster**: only leaders/standalone nodes sit on the hash ring; each shard can have its own followers.

## Example

```java
ClusterManager cluster = new ClusterManager(50);
cluster.addShard(ClusterNode.leader("shard-1", 1000, Path.of("data/s1"), true));
cluster.addShard(ClusterNode.leader("shard-2", 1000, Path.of("data/s2"), true));

cluster.set("user:1", "ankit");
cluster.get("user:1"); // routed to owning shard
```

---

Built as a learning / resume project — not a production Redis replacement.
