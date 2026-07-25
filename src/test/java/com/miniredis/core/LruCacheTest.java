package com.miniredis.core;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LruCacheTest {

    @Test
    void setGet_returnsValue() {
        LruCache cache = new LruCache(10, Clock.systemUTC());
        cache.set("k", "v");
        assertEquals("v", cache.get("k").orElseThrow());
    }

    @Test
    void set_whenOverCapacity_evictsLeastRecentlyUsed() {
        LruCache cache = new LruCache(2, Clock.systemUTC());
        cache.set("a", "1");
        cache.set("b", "2");
        cache.get("a");
        cache.set("c", "3");

        assertTrue(cache.get("a").isPresent());
        assertFalse(cache.get("b").isPresent());
        assertTrue(cache.get("c").isPresent());
    }

    @Test
    void get_whenExpired_returnsEmpty() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        LruCache cache = new LruCache(10, clock);
        cache.set("k", "v", Duration.ofSeconds(5));

        clock.advance(Duration.ofSeconds(6));
        assertTrue(cache.get("k").isEmpty());
    }

    @Test
    void delete_removesKey() {
        LruCache cache = new LruCache(10, Clock.systemUTC());
        cache.set("k", "v");
        assertTrue(cache.delete("k"));
        assertTrue(cache.get("k").isEmpty());
    }

    @Test
    void set_blankKey_throws() {
        LruCache cache = new LruCache(10, Clock.systemUTC());
        assertThrows(IllegalArgumentException.class, () -> cache.set(" ", "v"));
    }

    static final class MutableClock extends Clock {
        private Instant instant;

        MutableClock(Instant instant) {
            this.instant = instant;
        }

        void advance(Duration d) {
            instant = instant.plus(d);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}
