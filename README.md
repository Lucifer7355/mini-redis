# Mini Redis

Redis jaisa chota distributed cache, Java 21 me. Resume / learning ke liye banaya - asli Redis ka replacement nahi.

Jo hai:
- LRU eviction
- TTL
- Snapshot (RDB style) + AOF
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
- [LLD.md](./LLD.md) - classes / details

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

## Quick example

```java
ClusterManager cluster = new ClusterManager(50);
cluster.addShard(ClusterNode.leader("shard-1", 1000, Path.of("data/s1"), true));
cluster.addShard(ClusterNode.leader("shard-2", 1000, Path.of("data/s2"), true));

cluster.set("user:1", "ankit");
cluster.get("user:1");
```
