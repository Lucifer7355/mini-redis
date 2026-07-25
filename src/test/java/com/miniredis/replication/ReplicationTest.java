package com.miniredis.replication;

import com.miniredis.core.LruCache;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationTest {

    @Test
    void attach_fullSyncThenIncrementalReplicates() {
        LruCache leaderCache = new LruCache(50, Clock.systemUTC());
        leaderCache.set("seed", "1");

        ReplicationLeader leader = new ReplicationLeader(leaderCache);
        LruCache followerCache = new LruCache(50, Clock.systemUTC());
        ReplicationFollower follower = new ReplicationFollower("f1", followerCache);

        leader.attach(follower);
        assertEquals("1", follower.get("seed").orElseThrow());

        leaderCache.set("k", "v");
        leader.replicateSet("k", "v");
        leaderCache.set("t", "x", Duration.ofMinutes(1));
        leader.replicateSetEx("t", "x", Duration.ofMinutes(1));
        leaderCache.delete("seed");
        leader.replicateDel("seed");

        assertEquals("v", follower.get("k").orElseThrow());
        assertEquals("x", follower.get("t").orElseThrow());
        assertTrue(follower.get("seed").isEmpty());
        assertEquals(leader.currentSequence(), follower.appliedSequence());
    }
}
