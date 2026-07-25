package com.miniredis.persistence;

import com.miniredis.core.LruCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistenceTest {

    @TempDir
    Path temp;

    @Test
    void snapshot_saveAndLoad_restoresEntries() throws Exception {
        LruCache original = new LruCache(50, Clock.systemUTC());
        original.set("a", "1");
        original.set("b", "2", Duration.ofHours(1));

        SnapshotStore store = new SnapshotStore(temp.resolve("dump.rdb"));
        store.save(original);

        LruCache restored = new LruCache(50, Clock.systemUTC());
        store.load(restored);

        assertEquals("1", restored.get("a").orElseThrow());
        assertEquals("2", restored.get("b").orElseThrow());
    }

    @Test
    void aof_replay_restoresMutations() throws Exception {
        Path aof = temp.resolve("appendonly.aof");
        LruCache cache = new LruCache(50, Clock.systemUTC());

        try (AofLog log = new AofLog(aof)) {
            log.appendSet("user", "ankit");
            log.appendSetEx("sess", 60_000, "tok");
            log.appendDel("user");
        }

        LruCache replayed = new LruCache(50, Clock.systemUTC());
        try (AofLog log = new AofLog(aof)) {
            log.replay(replayed);
        }

        assertTrue(replayed.get("user").isEmpty());
        assertEquals("tok", replayed.get("sess").orElseThrow());
    }

    @Test
    void persistenceManager_recover_loadsSnapshotThenAof() throws Exception {
        Path data = temp.resolve("data");
        LruCache cache = new LruCache(50, Clock.systemUTC());
        try (PersistenceManager pm = new PersistenceManager(cache, data, true)) {
            cache.set("k", "from-memory");
            pm.onSet("k", "from-memory");
            pm.saveSnapshot();
            cache.set("k", "after-snapshot");
            pm.onSet("k", "after-snapshot");
        }

        LruCache recovered = new LruCache(50, Clock.systemUTC());
        try (PersistenceManager pm = new PersistenceManager(recovered, data, true)) {
            pm.recover();
        }
        assertEquals("after-snapshot", recovered.get("k").orElseThrow());
    }
}
