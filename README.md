# Mini Redis

Small Redis-like distributed cache in Java 21. Built for learning / resume - not a drop-in Redis replacement.

Features:
- LRU eviction
- TTL
- Snapshot (RDB-style) + AOF
- Pub/Sub
- Leader-follower replication
- Consistent hashing + cluster routing

## Run

```bash
mvn test
mvn -q exec:java
```

## Docs

- [HLD.md](./HLD.md) - overall design
- [LLD.md](./LLD.md) - classes and details

## Layout

```
core/          cache + ttl
persistence/   dump.rdb + aof
pubsub/
replication/   leader / follower
sharding/      consistent hash
cluster/       node + manager
Main.java      demo
```

## Example

```java
ClusterManager cluster = new ClusterManager(50);
cluster.addShard(ClusterNode.leader("shard-1", 1000, Path.of("data/s1"), true));
cluster.addShard(ClusterNode.leader("shard-2", 1000, Path.of("data/s2"), true));

cluster.set("user:1", "ankit");
cluster.get("user:1");
```
