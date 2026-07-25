package com.miniredis.core;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe LRU cache with per-key TTL.
 * Access order LinkedHashMap handles eviction; RW lock keeps reads cheap.
 */
public final class LruCache {

    private final int capacity;
    private final Clock clock;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final LinkedHashMap<String, CacheEntry> store;

    public LruCache(int capacity, Clock clock) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
        this.clock = Objects.requireNonNull(clock, "clock");
        this.store = new LinkedHashMap<>(16, 0.75f, true);
    }

    public void set(String key, String value) {
        set(key, value, null);
    }

    public void set(String key, String value, Duration ttl) {
        validateKey(key);
        Objects.requireNonNull(value, "value");
        Instant expiresAt = ttl == null ? null : clock.instant().plus(ttl);
        CacheEntry entry = expiresAt == null
                ? CacheEntry.permanent(value)
                : CacheEntry.withTtl(value, expiresAt);

        lock.writeLock().lock();
        try {
            purgeExpiredLocked();
            store.put(key, entry);
            evictWhileOverCapacityLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<String> get(String key) {
        validateKey(key);
        lock.writeLock().lock();
        try {
            CacheEntry entry = store.get(key);
            if (entry == null) {
                return Optional.empty();
            }
            if (entry.isExpired(clock.instant())) {
                store.remove(key);
                return Optional.empty();
            }
            // LinkedHashMap access-order update happens on get
            return Optional.of(entry.value());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean delete(String key) {
        validateKey(key);
        lock.writeLock().lock();
        try {
            return store.remove(key) != null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean exists(String key) {
        return get(key).isPresent();
    }

    public int size() {
        lock.writeLock().lock();
        try {
            purgeExpiredLocked();
            return store.size();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int capacity() {
        return capacity;
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            store.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Snapshot of live (non-expired) entries for persistence / replication. */
    public Map<String, CacheEntry> snapshot() {
        lock.writeLock().lock();
        try {
            purgeExpiredLocked();
            return new HashMap<>(store);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void loadAll(Map<String, CacheEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        lock.writeLock().lock();
        try {
            store.clear();
            Instant now = clock.instant();
            for (Map.Entry<String, CacheEntry> e : entries.entrySet()) {
                if (!e.getValue().isExpired(now)) {
                    store.put(e.getKey(), e.getValue());
                }
            }
            evictWhileOverCapacityLocked();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<String> keys() {
        lock.writeLock().lock();
        try {
            purgeExpiredLocked();
            return new ArrayList<>(store.keySet());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void purgeExpiredLocked() {
        Instant now = clock.instant();
        store.entrySet().removeIf(e -> e.getValue().isExpired(now));
    }

    private void evictWhileOverCapacityLocked() {
        while (store.size() > capacity) {
            String eldest = store.keySet().iterator().next();
            store.remove(eldest);
        }
    }

    private static void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be blank");
        }
    }
}
