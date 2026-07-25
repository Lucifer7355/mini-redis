package com.miniredis.sharding;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsistentHashRingTest {

    @Test
    void route_sameKey_sameNode() {
        ConsistentHashRing<String> ring = new ConsistentHashRing<>(40);
        ring.addNode("n1");
        ring.addNode("n2");
        ring.addNode("n3");

        String first = ring.route("user:42");
        for (int i = 0; i < 20; i++) {
            assertEquals(first, ring.route("user:42"));
        }
    }

    @Test
    void route_spreadsKeysAcrossNodes() {
        ConsistentHashRing<String> ring = new ConsistentHashRing<>(50);
        ring.addNode("a");
        ring.addNode("b");
        ring.addNode("c");

        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 300; i++) {
            String node = ring.route("key-" + i);
            counts.merge(node, 1, Integer::sum);
        }

        assertEquals(3, counts.size());
        for (int c : counts.values()) {
            assertTrue(c > 40, "expected reasonably even distribution, got " + counts);
        }
    }

    @Test
    void removeNode_stillRoutes() {
        ConsistentHashRing<String> ring = new ConsistentHashRing<>(20);
        ring.addNode("a");
        ring.addNode("b");
        ring.removeNode("a");
        assertNotNull(ring.route("anything"));
        assertEquals(1, ring.size());
    }
}
