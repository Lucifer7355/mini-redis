package com.miniredis.cluster;

import com.miniredis.sharding.ConsistentHashRing;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Cluster facade: routes keys via consistent hashing across shard leaders.
 */
public final class ClusterManager implements AutoCloseable {

    private final ConsistentHashRing<ClusterNode> ring;
    private final List<ClusterNode> nodes = new ArrayList<>();

    public ClusterManager(int virtualNodes) {
        this.ring = new ConsistentHashRing<>(virtualNodes);
    }

    public void addShard(ClusterNode node) {
        Objects.requireNonNull(node, "node");
        if (node.role() == ClusterNode.Role.FOLLOWER) {
            throw new IllegalArgumentException("only leaders/standalone shards join the ring");
        }
        nodes.add(node);
        ring.addNode(node);
    }

    public void removeShard(ClusterNode node) {
        Objects.requireNonNull(node, "node");
        ring.removeNode(node);
        nodes.remove(node);
    }

    public ClusterNode locate(String key) {
        return ring.route(key);
    }

    public void set(String key, String value) throws IOException {
        locate(key).set(key, value);
    }

    public void set(String key, String value, Duration ttl) throws IOException {
        locate(key).set(key, value, ttl);
    }

    public Optional<String> get(String key) {
        return locate(key).get(key);
    }

    public boolean delete(String key) throws IOException {
        return locate(key).delete(key);
    }

    public int totalKeys() {
        return nodes.stream().mapToInt(ClusterNode::size).sum();
    }

    public List<ClusterNode> shards() {
        return List.copyOf(nodes);
    }

    public int shardCount() {
        return ring.size();
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (ClusterNode node : nodes) {
            try {
                node.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                } else {
                    first.addSuppressed(e);
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
