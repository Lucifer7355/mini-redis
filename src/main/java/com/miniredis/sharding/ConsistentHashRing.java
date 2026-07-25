package com.miniredis.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent hashing ring with virtual nodes for even shard distribution.
 */
public final class ConsistentHashRing<T> {

    private final int virtualNodes;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final NavigableMap<Long, T> ring = new TreeMap<>();

    public ConsistentHashRing(int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be > 0");
        }
        this.virtualNodes = virtualNodes;
    }

    public void addNode(T node) {
        Objects.requireNonNull(node, "node");
        lock.writeLock().lock();
        try {
            for (int i = 0; i < virtualNodes; i++) {
                long hash = hash(node.toString() + "#" + i);
                ring.put(hash, node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeNode(T node) {
        Objects.requireNonNull(node, "node");
        lock.writeLock().lock();
        try {
            for (int i = 0; i < virtualNodes; i++) {
                long hash = hash(node.toString() + "#" + i);
                ring.remove(hash, node);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public T route(String key) {
        Objects.requireNonNull(key, "key");
        lock.readLock().lock();
        try {
            if (ring.isEmpty()) {
                throw new IllegalStateException("hash ring is empty");
            }
            long hash = hash(key);
            Map.Entry<Long, T> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();
            }
            return entry.getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<T> nodes() {
        lock.readLock().lock();
        try {
            return new ArrayList<>(ring.values().stream().distinct().toList());
        } finally {
            lock.readLock().unlock();
        }
    }

    public int size() {
        lock.readLock().lock();
        try {
            return (int) ring.values().stream().distinct().count();
        } finally {
            lock.readLock().unlock();
        }
    }

    private static long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            long value = 0;
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (digest[i] & 0xffL);
            }
            return value;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }
}
