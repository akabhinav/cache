package com.distributedcache.redis;

import com.distributedcache.redis.cluster.ConsistentHash;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashTest {

    private ConsistentHash<String> consistentHash;

    @BeforeEach
    void setUp() {
        consistentHash = new ConsistentHash<>(100);
    }

    @Test
    void testAddAndGetNode() {
        consistentHash.addNode("node1");
        consistentHash.addNode("node2");
        consistentHash.addNode("node3");

        String node = consistentHash.getNode("some-key");
        assertNotNull(node);
        assertTrue(consistentHash.getAllNodes().contains(node));
    }

    @Test
    void testRemoveNode() {
        consistentHash.addNode("node1");
        consistentHash.addNode("node2");

        assertEquals(2, consistentHash.size());

        consistentHash.removeNode("node1");

        assertEquals(1, consistentHash.size());
        assertEquals("node2", consistentHash.getNode("any-key"));
    }

    @Test
    void testDistribution() {
        consistentHash.addNode("node1");
        consistentHash.addNode("node2");
        consistentHash.addNode("node3");

        Map<String, Integer> distribution = new HashMap<>();

        // Test 1000 keys
        for (int i = 0; i < 1000; i++) {
            String node = consistentHash.getNode("key-" + i);
            distribution.merge(node, 1, Integer::sum);
        }

        // Each node should have some keys (rough distribution check)
        assertEquals(3, distribution.size());
        for (Integer count : distribution.values()) {
            assertTrue(count > 100, "Node should have at least 100 keys");
        }
    }

    @Test
    void testConsistency() {
        consistentHash.addNode("node1");
        consistentHash.addNode("node2");
        consistentHash.addNode("node3");

        String key = "test-key";
        String originalNode = consistentHash.getNode(key);

        // Remove a different node
        consistentHash.removeNode("node1");

        // Key should still map to same node (if it wasn't node1)
        if (!originalNode.equals("node1")) {
            assertEquals(originalNode, consistentHash.getNode(key));
        }
    }
}
