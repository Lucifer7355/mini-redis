package com.miniredis.cluster;

import com.miniredis.core.LruCache;
import com.miniredis.persistence.PersistenceManager;
import com.miniredis.pubsub.PubSubHub;
import com.miniredis.replication.ReplicationFollower;
import com.miniredis.replication.ReplicationLeader;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * One cluster shard: local LRU cache + optional persistence, pub/sub, and replication.
 */
public final class ClusterNode implements AutoCloseable {

    public enum Role {
        LEADER,
        FOLLOWER,
        STANDALONE
    }

    private final String nodeId;
    private final LruCache cache;
    private final PubSubHub pubSub;
    private final PersistenceManager persistence;
    private final Role role;
    private final ReplicationLeader leader;
    private final ReplicationFollower follower;

    private ClusterNode(
            String nodeId,
            LruCache cache,
            PubSubHub pubSub,
            PersistenceManager persistence,
            Role role,
            ReplicationLeader leader,
            ReplicationFollower follower) {
        this.nodeId = nodeId;
        this.cache = cache;
        this.pubSub = pubSub;
        this.persistence = persistence;
        this.role = role;
        this.leader = leader;
        this.follower = follower;
    }

    public static ClusterNode standalone(String nodeId, int capacity, Path dataDir, boolean aof)
            throws IOException {
        LruCache cache = new LruCache(capacity, Clock.systemUTC());
        PersistenceManager pm = new PersistenceManager(cache, dataDir, aof);
        pm.recover();
        return new ClusterNode(nodeId, cache, new PubSubHub(), pm, Role.STANDALONE, null, null);
    }

    public static ClusterNode leader(String nodeId, int capacity, Path dataDir, boolean aof)
            throws IOException {
        LruCache cache = new LruCache(capacity, Clock.systemUTC());
        PersistenceManager pm = new PersistenceManager(cache, dataDir, aof);
        pm.recover();
        ReplicationLeader repl = new ReplicationLeader(cache);
        return new ClusterNode(nodeId, cache, new PubSubHub(), pm, Role.LEADER, repl, null);
    }

    public static ClusterNode follower(String nodeId, int capacity) {
        LruCache cache = new LruCache(capacity, Clock.systemUTC());
        ReplicationFollower f = new ReplicationFollower(nodeId, cache);
        return new ClusterNode(nodeId, cache, new PubSubHub(), null, Role.FOLLOWER, null, f);
    }

    public String nodeId() {
        return nodeId;
    }

    public Role role() {
        return role;
    }

    public void set(String key, String value) throws IOException {
        ensureWritable();
        cache.set(key, value);
        if (persistence != null) {
            persistence.onSet(key, value);
        }
        if (leader != null) {
            leader.replicateSet(key, value);
        }
    }

    public void set(String key, String value, Duration ttl) throws IOException {
        ensureWritable();
        Objects.requireNonNull(ttl, "ttl");
        cache.set(key, value, ttl);
        if (persistence != null) {
            persistence.onSetEx(key, ttl, value);
        }
        if (leader != null) {
            leader.replicateSetEx(key, value, ttl);
        }
    }

    public Optional<String> get(String key) {
        return cache.get(key);
    }

    public boolean delete(String key) throws IOException {
        ensureWritable();
        boolean removed = cache.delete(key);
        if (removed) {
            if (persistence != null) {
                persistence.onDelete(key);
            }
            if (leader != null) {
                leader.replicateDel(key);
            }
        }
        return removed;
    }

    public int size() {
        return cache.size();
    }

    public PubSubHub pubSub() {
        return pubSub;
    }

    public ReplicationLeader asLeader() {
        if (leader == null) {
            throw new IllegalStateException(nodeId + " is not a leader");
        }
        return leader;
    }

    public ReplicationFollower asFollower() {
        if (follower == null) {
            throw new IllegalStateException(nodeId + " is not a follower");
        }
        return follower;
    }

    public void saveSnapshot() throws IOException {
        if (persistence == null) {
            throw new IllegalStateException("persistence not enabled on " + nodeId);
        }
        persistence.saveSnapshot();
    }

    public void rewriteAof() throws IOException {
        if (persistence == null) {
            throw new IllegalStateException("persistence not enabled on " + nodeId);
        }
        persistence.rewriteAof();
    }

    public LruCache cache() {
        return cache;
    }

    private void ensureWritable() {
        if (role == Role.FOLLOWER) {
            throw new IllegalStateException("follower is read-only: " + nodeId);
        }
    }

    @Override
    public void close() throws IOException {
        if (persistence != null) {
            persistence.close();
        }
    }

    @Override
    public String toString() {
        return nodeId;
    }
}
