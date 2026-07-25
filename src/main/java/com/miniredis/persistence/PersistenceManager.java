package com.miniredis.persistence;

import com.miniredis.core.LruCache;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Combines snapshot + AOF. Startup: load snapshot, then replay AOF.
 * Writes always go to AOF; snapshot can be taken on demand.
 */
public final class PersistenceManager implements AutoCloseable {

    private final LruCache cache;
    private final SnapshotStore snapshotStore;
    private final AofLog aofLog;
    private final boolean aofEnabled;

    public PersistenceManager(LruCache cache, Path dataDir, boolean aofEnabled) throws IOException {
        this.cache = Objects.requireNonNull(cache, "cache");
        Objects.requireNonNull(dataDir, "dataDir");
        this.aofEnabled = aofEnabled;
        this.snapshotStore = new SnapshotStore(dataDir.resolve("dump.rdb"));
        this.aofLog = aofEnabled ? new AofLog(dataDir.resolve("appendonly.aof")) : null;
    }

    public void recover() throws IOException {
        snapshotStore.load(cache);
        if (aofLog != null) {
            aofLog.replay(cache);
        }
    }

    public void onSet(String key, String value) throws IOException {
        if (aofLog != null) {
            aofLog.appendSet(key, value);
        }
    }

    public void onSetEx(String key, Duration ttl, String value) throws IOException {
        if (aofLog != null) {
            aofLog.appendSetEx(key, ttl.toMillis(), value);
        }
    }

    public void onDelete(String key) throws IOException {
        if (aofLog != null) {
            aofLog.appendDel(key);
        }
    }

    public void saveSnapshot() throws IOException {
        snapshotStore.save(cache);
    }

    public void rewriteAof() throws IOException {
        if (aofLog != null) {
            aofLog.rewrite(cache);
        }
    }

    public SnapshotStore snapshotStore() {
        return snapshotStore;
    }

    @Override
    public void close() throws IOException {
        if (aofLog != null) {
            aofLog.close();
        }
    }
}
