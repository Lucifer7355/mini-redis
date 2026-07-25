package com.miniredis.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * In-process pub/sub. Subscribers register a callback per channel.
 */
public final class PubSubHub {

    private final Map<String, CopyOnWriteArrayList<BiConsumer<String, String>>> channels =
            new ConcurrentHashMap<>();

    public void subscribe(String channel, BiConsumer<String, String> listener) {
        validateChannel(channel);
        Objects.requireNonNull(listener, "listener");
        channels.computeIfAbsent(channel, c -> new CopyOnWriteArrayList<>()).add(listener);
    }

    public boolean unsubscribe(String channel, BiConsumer<String, String> listener) {
        validateChannel(channel);
        Objects.requireNonNull(listener, "listener");
        CopyOnWriteArrayList<BiConsumer<String, String>> list = channels.get(channel);
        if (list == null) {
            return false;
        }
        boolean removed = list.remove(listener);
        if (list.isEmpty()) {
            channels.remove(channel, list);
        }
        return removed;
    }

    public int publish(String channel, String message) {
        validateChannel(channel);
        Objects.requireNonNull(message, "message");
        CopyOnWriteArrayList<BiConsumer<String, String>> list = channels.get(channel);
        if (list == null || list.isEmpty()) {
            return 0;
        }
        for (BiConsumer<String, String> listener : list) {
            listener.accept(channel, message);
        }
        return list.size();
    }

    public int subscriberCount(String channel) {
        validateChannel(channel);
        CopyOnWriteArrayList<BiConsumer<String, String>> list = channels.get(channel);
        return list == null ? 0 : list.size();
    }

    public List<String> channels() {
        return new ArrayList<>(channels.keySet());
    }

    private static void validateChannel(String channel) {
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException("channel must not be blank");
        }
    }
}
