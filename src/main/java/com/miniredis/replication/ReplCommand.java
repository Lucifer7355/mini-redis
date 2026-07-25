package com.miniredis.replication;

import java.time.Duration;
import java.util.Objects;

/**
 * Replication log entry mirrored from leader to followers.
 */
public sealed interface ReplCommand permits ReplCommand.Set, ReplCommand.SetEx, ReplCommand.Del {

    long sequence();

    record Set(long sequence, String key, String value) implements ReplCommand {
        public Set {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }
    }

    record SetEx(long sequence, String key, String value, Duration ttl) implements ReplCommand {
        public SetEx {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
            Objects.requireNonNull(ttl, "ttl");
        }
    }

    record Del(long sequence, String key) implements ReplCommand {
        public Del {
            Objects.requireNonNull(key, "key");
        }
    }
}
