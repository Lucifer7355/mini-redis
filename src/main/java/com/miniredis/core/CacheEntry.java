package com.miniredis.core;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Single cache value with optional absolute expiry time.
 */
public final class CacheEntry {

    private final String value;
    private final Instant expiresAt;

    public CacheEntry(String value, Instant expiresAt) {
        this.value = Objects.requireNonNull(value, "value");
        this.expiresAt = expiresAt;
    }

    public static CacheEntry permanent(String value) {
        return new CacheEntry(value, null);
    }

    public static CacheEntry withTtl(String value, Instant expiresAt) {
        return new CacheEntry(value, Objects.requireNonNull(expiresAt, "expiresAt"));
    }

    public String value() {
        return value;
    }

    public Optional<Instant> expiresAt() {
        return Optional.ofNullable(expiresAt);
    }

    public boolean isExpired(Instant now) {
        return expiresAt != null && !expiresAt.isAfter(now);
    }

    public CacheEntry withValue(String newValue) {
        return new CacheEntry(newValue, expiresAt);
    }
}
