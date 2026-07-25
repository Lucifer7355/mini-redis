package com.miniredis.replication;

import com.miniredis.core.CacheEntry;
import com.miniredis.core.LruCache;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Read-only follower. Applies replicated commands; rejects local writes.
 */
public final class ReplicationFollower {

    private final String id;
    private final LruCache cache;
    private final AtomicLong appliedSequence = new AtomicLong(0);

    public ReplicationFollower(String id, LruCache cache) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id must not be blank");
        }
        this.id = id;
        this.cache = Objects.requireNonNull(cache, "cache");
    }

    public String id() {
        return id;
    }

    public Optional<String> get(String key) {
        return cache.get(key);
    }

    public int size() {
        return cache.size();
    }

    public long appliedSequence() {
        return appliedSequence.get();
    }

    public void fullSync(Map<String, CacheEntry> snapshot, long leaderSequence) {
        Objects.requireNonNull(snapshot, "snapshot");
        cache.loadAll(snapshot);
        appliedSequence.set(leaderSequence);
    }

    public void apply(ReplCommand command) {
        Objects.requireNonNull(command, "command");
        if (command.sequence() <= appliedSequence.get()) {
            return;
        }
        switch (command) {
            case ReplCommand.Set set -> cache.set(set.key(), set.value());
            case ReplCommand.SetEx setEx -> cache.set(setEx.key(), setEx.value(), setEx.ttl());
            case ReplCommand.Del del -> cache.delete(del.key());
        }
        appliedSequence.set(command.sequence());
    }

    public LruCache cache() {
        return cache;
    }
}
