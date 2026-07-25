package com.miniredis.persistence;

import com.miniredis.core.LruCache;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Append-Only File log. Every mutating command is written as a line:
 * SET key value
 * SETEX key ttlMillis value
 * DEL key
 */
public final class AofLog implements AutoCloseable {

    private final Path file;
    private final ReentrantLock lock = new ReentrantLock();
    private BufferedWriter writer;

    public AofLog(Path file) throws IOException {
        this.file = Objects.requireNonNull(file, "file");
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        this.writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public Path file() {
        return file;
    }

    public void appendSet(String key, String value) throws IOException {
        appendLine("SET " + encode(key) + " " + encode(value));
    }

    public void appendSetEx(String key, long ttlMillis, String value) throws IOException {
        appendLine("SETEX " + encode(key) + " " + ttlMillis + " " + encode(value));
    }

    public void appendDel(String key) throws IOException {
        appendLine("DEL " + encode(key));
    }

    public void replay(LruCache cache) throws IOException {
        Objects.requireNonNull(cache, "cache");
        if (!Files.exists(file)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                applyLine(cache, line);
            }
        }
    }

    public void rewrite(LruCache cache) throws IOException {
        Objects.requireNonNull(cache, "cache");
        Path tmp = file.resolveSibling(file.getFileName() + ".rewrite");
        try (BufferedWriter out = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
            for (var e : cache.snapshot().entrySet()) {
                if (e.getValue().expiresAt().isPresent()) {
                    long remaining = Duration.between(
                            java.time.Instant.now(),
                            e.getValue().expiresAt().get()).toMillis();
                    if (remaining <= 0) {
                        continue;
                    }
                    out.write("SETEX " + encode(e.getKey()) + " " + remaining + " " + encode(e.getValue().value()));
                } else {
                    out.write("SET " + encode(e.getKey()) + " " + encode(e.getValue().value()));
                }
                out.newLine();
            }
        }
        lock.lock();
        try {
            closeWriter();
            try {
                Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException ignored) {
                Files.move(tmp, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } finally {
            lock.unlock();
        }
    }

    private void appendLine(String line) throws IOException {
        lock.lock();
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } finally {
            lock.unlock();
        }
    }

    private static void applyLine(LruCache cache, String line) {
        String[] parts = tokenize(line);
        if (parts.length == 0) {
            return;
        }
        switch (parts[0]) {
            case "SET" -> {
                if (parts.length != 3) {
                    throw new IllegalStateException("bad SET line: " + line);
                }
                cache.set(decode(parts[1]), decode(parts[2]));
            }
            case "SETEX" -> {
                if (parts.length != 4) {
                    throw new IllegalStateException("bad SETEX line: " + line);
                }
                long ttl = Long.parseLong(parts[2]);
                cache.set(decode(parts[1]), decode(parts[3]), Duration.ofMillis(ttl));
            }
            case "DEL" -> {
                if (parts.length != 2) {
                    throw new IllegalStateException("bad DEL line: " + line);
                }
                cache.delete(decode(parts[1]));
            }
            default -> throw new IllegalStateException("unknown AOF command: " + parts[0]);
        }
    }

    private static String encode(String s) {
        return s.replace("\\", "\\\\").replace(" ", "\\s").replace("\n", "\\n");
    }

    private static String decode(String s) {
        return s.replace("\\n", "\n").replace("\\s", " ").replace("\\\\", "\\");
    }

    private static String[] tokenize(String line) {
        java.util.List<String> tokens = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean escape = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (escape) {
                cur.append('\\').append(c);
                escape = false;
            } else if (c == '\\') {
                escape = true;
            } else if (c == ' ') {
                tokens.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (escape) {
            cur.append('\\');
        }
        if (!cur.isEmpty() || !tokens.isEmpty()) {
            tokens.add(cur.toString());
        }
        return tokens.toArray(String[]::new);
    }

    private void closeWriter() throws IOException {
        if (writer != null) {
            writer.close();
            writer = null;
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            closeWriter();
        } finally {
            lock.unlock();
        }
    }
}
