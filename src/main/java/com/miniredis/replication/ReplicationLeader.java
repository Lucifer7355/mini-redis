package com.miniredis.replication;

import com.miniredis.core.CacheEntry;
import com.miniredis.core.LruCache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leader replicates writes to attached followers (async, in-process).
 */
public final class ReplicationLeader {

    private final LruCache cache;
    private final AtomicLong sequence = new AtomicLong(0);
    private final CopyOnWriteArrayList<ReplicationFollower> followers = new CopyOnWriteArrayList<>();

    public ReplicationLeader(LruCache cache) {
        this.cache = Objects.requireNonNull(cache, "cache");
    }

    public void attach(ReplicationFollower follower) {
        Objects.requireNonNull(follower, "follower");
        followers.add(follower);
        Map<String, CacheEntry> snap = cache.snapshot();
        follower.fullSync(snap, sequence.get());
    }

    public void detach(ReplicationFollower follower) {
        followers.remove(follower);
    }

    public void replicateSet(String key, String value) {
        long seq = sequence.incrementAndGet();
        broadcast(new ReplCommand.Set(seq, key, value));
    }

    public void replicateSetEx(String key, String value, Duration ttl) {
        long seq = sequence.incrementAndGet();
        broadcast(new ReplCommand.SetEx(seq, key, value, ttl));
    }

    public void replicateDel(String key) {
        long seq = sequence.incrementAndGet();
        broadcast(new ReplCommand.Del(seq, key));
    }

    public long currentSequence() {
        return sequence.get();
    }

    public List<ReplicationFollower> followers() {
        return new ArrayList<>(followers);
    }

    private void broadcast(ReplCommand command) {
        for (ReplicationFollower follower : followers) {
            follower.apply(command);
        }
    }
}
