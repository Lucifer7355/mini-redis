package com.miniredis;

import com.miniredis.cluster.ClusterManager;
import com.miniredis.cluster.ClusterNode;
import com.miniredis.core.LruCache;
import com.miniredis.pubsub.PubSubHub;
import com.miniredis.replication.ReplicationFollower;
import com.miniredis.replication.ReplicationLeader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

/**
 * Runs scenarios that exercise Mini Redis features end to end.
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        Path base = Files.createTempDirectory("miniredis-demo");
        System.out.println("data dir: " + base.toAbsolutePath());
        System.out.println();

        demoLruAndTtl();
        demoPersistence(base.resolve("persist"));
        demoPubSub();
        demoReplication();
        demoShardingCluster(base.resolve("cluster"));

        System.out.println();
        System.out.println("=== all demos done ===");
    }

    private static void demoLruAndTtl() throws InterruptedException {
        header("LRU + TTL");
        LruCache cache = new LruCache(3, Clock.systemUTC());

        action("SET a=1, b=2, c=3 (capacity=3)");
        cache.set("a", "1");
        cache.set("b", "2");
        cache.set("c", "3");
        result("size=" + cache.size() + " keys=" + cache.keys());

        action("GET a (marks a as recently used), then SET d=4");
        cache.get("a");
        cache.set("d", "4");
        result("keys=" + cache.keys() + " (b should be evicted — least recently used)");
        why("LinkedHashMap access-order + capacity eviction");

        action("SET temp=x with TTL 100ms, wait 150ms, GET temp");
        cache.set("temp", "x", Duration.ofMillis(100));
        Thread.sleep(150);
        result("temp present? " + cache.get("temp").isPresent());
        why("expired entries are purged on read/write");
        System.out.println();
    }

    private static void demoPersistence(Path dir) throws Exception {
        header("Snapshot (RDB) + AOF");
        Files.createDirectories(dir);

        ClusterNode node = ClusterNode.standalone("n1", 100, dir, true);
        action("SET user:1=ankit, SETEX session=token TTL=1h, SAVE snapshot");
        node.set("user:1", "ankit");
        node.set("session", "token", Duration.ofHours(1));
        node.saveSnapshot();
        result("keys on disk: dump.rdb + appendonly.aof under " + dir);
        node.close();

        action("restart node and recover from snapshot + AOF replay");
        ClusterNode restarted = ClusterNode.standalone("n1", 100, dir, true);
        result("user:1=" + restarted.get("user:1").orElse("MISS")
                + " session=" + restarted.get("session").orElse("MISS"));
        why("snapshot is point-in-time; AOF catches writes after last snapshot");

        action("AOF rewrite (compact log from live dataset)");
        restarted.rewriteAof();
        result("appendonly.aof rewritten");
        restarted.close();
        System.out.println();
    }

    private static void demoPubSub() {
        header("Pub/Sub");
        PubSubHub hub = new PubSubHub();
        List<String> inbox = new CopyOnWriteArrayList<>();
        BiConsumer<String, String> listener = (ch, msg) -> inbox.add(ch + " => " + msg);

        action("SUBSCRIBE news, PUBLISH news 'hello'");
        hub.subscribe("news", listener);
        int receivers = hub.publish("news", "hello");
        result("delivered to " + receivers + " subscriber(s): " + inbox);
        why("fan-out to all channel listeners without storing the message");
        System.out.println();
    }

    private static void demoReplication() throws Exception {
        header("Leader / Follower replication");
        Path dir = Files.createTempDirectory("miniredis-repl");
        ClusterNode leaderNode = ClusterNode.leader("leader", 100, dir, false);
        ClusterNode followerNode = ClusterNode.follower("follower-1", 100);

        ReplicationLeader leader = leaderNode.asLeader();
        ReplicationFollower follower = followerNode.asFollower();

        action("attach follower (full sync), then SET on leader");
        leader.attach(follower);
        leaderNode.set("k1", "v1");
        leaderNode.set("k2", "v2", Duration.ofMinutes(5));

        result("leader size=" + leaderNode.size()
                + " follower size=" + follower.size()
                + " follower.k1=" + follower.get("k1").orElse("MISS")
                + " seq=" + follower.appliedSequence());
        why("writes go to leader; followers apply the replication stream");

        action("try SET on follower (should fail — read-only)");
        try {
            followerNode.set("x", "y");
            result("ERROR: write unexpectedly allowed");
        } catch (IllegalStateException e) {
            result("blocked: " + e.getMessage());
        }

        leaderNode.close();
        followerNode.close();
        System.out.println();
    }

    private static void demoShardingCluster(Path dir) throws Exception {
        header("Sharding + Cluster (consistent hashing)");
        Files.createDirectories(dir);
        ClusterManager cluster = new ClusterManager(50);

        ClusterNode s1 = ClusterNode.leader("shard-1", 200, dir.resolve("s1"), true);
        ClusterNode s2 = ClusterNode.leader("shard-2", 200, dir.resolve("s2"), true);
        ClusterNode s3 = ClusterNode.leader("shard-3", 200, dir.resolve("s3"), true);
        cluster.addShard(s1);
        cluster.addShard(s2);
        cluster.addShard(s3);

        action("SET 30 keys across 3 shards");
        for (int i = 0; i < 30; i++) {
            cluster.set("key-" + i, "val-" + i);
        }

        List<String> distribution = new ArrayList<>();
        for (ClusterNode shard : cluster.shards()) {
            distribution.add(shard.nodeId() + "=" + shard.size());
        }
        result("total=" + cluster.totalKeys() + " distribution " + distribution);

        action("GET key-7 routed to owning shard");
        String key = "key-7";
        ClusterNode owner = cluster.locate(key);
        Optional<String> value = cluster.get(key);
        result("key=" + key + " owner=" + owner.nodeId() + " value=" + value.orElse("MISS"));
        why("same key always hashes to the same shard via the ring");

        cluster.close();
        System.out.println();
    }

    private static void header(String title) {
        System.out.println("[SCENARIO] " + title);
    }

    private static void action(String text) {
        System.out.println("  [ACTION] " + text);
    }

    private static void result(String text) {
        System.out.println("  [RESULT] " + text);
    }

    private static void why(String text) {
        System.out.println("  [WHY]    " + text);
    }
}
