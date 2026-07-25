package com.miniredis.cluster;

import com.miniredis.pubsub.PubSubHub;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterIntegrationTest {

    @TempDir
    Path temp;

    @Test
    void cluster_routesAndPersistsPerShard() throws Exception {
        ClusterManager cluster = new ClusterManager(40);
        cluster.addShard(ClusterNode.leader("s1", 100, temp.resolve("s1"), true));
        cluster.addShard(ClusterNode.leader("s2", 100, temp.resolve("s2"), true));

        cluster.set("alpha", "1");
        cluster.set("beta", "2", Duration.ofMinutes(10));

        assertEquals("1", cluster.get("alpha").orElseThrow());
        assertEquals("2", cluster.get("beta").orElseThrow());
        assertEquals(2, cluster.totalKeys());
        cluster.close();
    }

    @Test
    void follower_rejectsWrites() throws Exception {
        ClusterNode follower = ClusterNode.follower("f1", 50);
        assertThrows(IllegalStateException.class, () -> follower.set("k", "v"));
        follower.close();
    }

    @Test
    void leaderFollower_syncsWrites() throws Exception {
        ClusterNode leader = ClusterNode.leader("L", 50, temp.resolve("L"), false);
        ClusterNode follower = ClusterNode.follower("F", 50);
        leader.asLeader().attach(follower.asFollower());

        leader.set("k", "v");
        assertEquals("v", follower.get("k").orElseThrow());
        leader.close();
        follower.close();
    }

    @Test
    void pubSub_deliversToSubscribers() {
        PubSubHub hub = new PubSubHub();
        List<String> msgs = new ArrayList<>();
        hub.subscribe("ch", (c, m) -> msgs.add(m));
        assertEquals(1, hub.publish("ch", "hi"));
        assertEquals(List.of("hi"), msgs);
    }
}
