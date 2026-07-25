package com.miniredis.persistence;

import com.miniredis.core.CacheEntry;
import com.miniredis.core.LruCache;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Point-in-time binary snapshot (RDB-style).
 * Format: magic + version + count + (key, value, expiresEpochMilli|-1)*
 */
public final class SnapshotStore {

    private static final int MAGIC = 0x4D524442; // MRDB
    private static final int VERSION = 1;

    private final Path file;

    public SnapshotStore(Path file) {
        this.file = Objects.requireNonNull(file, "file");
    }

    public Path file() {
        return file;
    }

    public void save(LruCache cache) throws IOException {
        Objects.requireNonNull(cache, "cache");
        Map<String, CacheEntry> data = cache.snapshot();
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(data.size());
            for (Map.Entry<String, CacheEntry> e : data.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue().value());
                out.writeLong(e.getValue().expiresAt().map(Instant::toEpochMilli).orElse(-1L));
            }
        }
        moveReplace(tmp, file);
    }

    private static void moveReplace(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ignored) {
            Files.move(from, to, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public void load(LruCache cache) throws IOException {
        Objects.requireNonNull(cache, "cache");
        if (!Files.exists(file)) {
            return;
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(file)))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IOException("invalid snapshot magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IOException("unsupported snapshot version: " + version);
            }
            int count = in.readInt();
            Map<String, CacheEntry> entries = new HashMap<>(count);
            for (int i = 0; i < count; i++) {
                String key = in.readUTF();
                String value = in.readUTF();
                long expiry = in.readLong();
                CacheEntry entry = expiry < 0
                        ? CacheEntry.permanent(value)
                        : CacheEntry.withTtl(value, Instant.ofEpochMilli(expiry));
                entries.put(key, entry);
            }
            cache.loadAll(entries);
        }
    }
}
