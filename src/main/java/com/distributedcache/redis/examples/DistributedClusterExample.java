package com.distributedcache.redis.examples;

import com.distributedcache.redis.cluster.ClusterNode;
import com.distributedcache.redis.cluster.DistributedRedisServer;

/**
 * Example of setting up a distributed Redis cluster with coordination
 *
 * This demonstrates:
 * - Starting multiple nodes
 * - Automatic leader election
 * - Gossip-based node discovery
 * - Data replication
 * - Failure detection
 */
public class DistributedClusterExample {

    public static void main(String[] args) throws Exception {
        System.out.println("=== Distributed Redis Cluster Example ===\n");

        // Create first node (seed node)
        System.out.println("Starting Node 1 (seed node)...");
        DistributedRedisServer node1 = new DistributedRedisServer.Builder()
            .host("localhost")
            .port(6379)
            .gossipPort(7379)
            .nodeId("node-1")
            .replicationFactor(2)
            .build();

        node1.start();
        Thread.sleep(2000); // Wait for node to stabilize

        System.out.println("\n" + node1.getClusterInfo());

        // Create second node, connecting to seed
        System.out.println("\n\nStarting Node 2...");
        DistributedRedisServer node2 = new DistributedRedisServer.Builder()
            .host("localhost")
            .port(6380)
            .gossipPort(7380)
            .nodeId("node-2")
            .replicationFactor(2)
            .addSeedNode("localhost", 7379, "node-1")  // Connect to node1's gossip port
            .build();

        node2.start();
        Thread.sleep(2000);

        System.out.println("\n" + node2.getClusterInfo());

        // Create third node
        System.out.println("\n\nStarting Node 3...");
        DistributedRedisServer node3 = new DistributedRedisServer.Builder()
            .host("localhost")
            .port(6381)
            .gossipPort(7381)
            .nodeId("node-3")
            .replicationFactor(2)
            .addSeedNode("localhost", 7379, "node-1")
            .build();

        node3.start();
        Thread.sleep(3000); // Wait for gossip to propagate

        // Show cluster state from all nodes
        System.out.println("\n\n=== CLUSTER STATE FROM ALL NODES ===\n");

        System.out.println("--- Node 1 View ---");
        System.out.println(node1.getClusterInfo());

        System.out.println("\n--- Node 2 View ---");
        System.out.println(node2.getClusterInfo());

        System.out.println("\n--- Node 3 View ---");
        System.out.println(node3.getClusterInfo());

        // Demonstrate leader election
        System.out.println("\n=== LEADER ELECTION ===");
        System.out.println("Node 1 is leader: " + node1.isLeader());
        System.out.println("Node 2 is leader: " + node2.isLeader());
        System.out.println("Node 3 is leader: " + node3.isLeader());

        // Demonstrate key routing
        System.out.println("\n=== KEY ROUTING ===");
        String[] testKeys = {"user:100", "user:200", "product:50", "session:abc"};

        for (String key : testKeys) {
            ClusterNode responsible1 = node1.getClusterManager().getNodeForKey(key);
            ClusterNode responsible2 = node2.getClusterManager().getNodeForKey(key);
            ClusterNode responsible3 = node3.getClusterManager().getNodeForKey(key);

            System.out.printf("Key '%s':\n", key);
            System.out.printf("  Node 1 says: %s\n", responsible1);
            System.out.printf("  Node 2 says: %s\n", responsible2);
            System.out.printf("  Node 3 says: %s\n", responsible3);
            System.out.printf("  All agree: %s\n\n",
                responsible1.equals(responsible2) && responsible2.equals(responsible3));
        }

        // Keep running for a while to observe gossip
        System.out.println("Cluster is running. Press Ctrl+C to stop.");
        System.out.println("Watch the logs to see heartbeats and gossip messages...\n");

        // Simulate node failure after 10 seconds
        Thread.sleep(10000);
        System.out.println("\n=== SIMULATING NODE FAILURE ===");
        System.out.println("Stopping Node 2...");
        node2.stop();

        // Wait for failure detection
        Thread.sleep(6000);

        System.out.println("\n--- Cluster state after Node 2 failure ---");
        System.out.println("Node 1 view:");
        System.out.println(node1.getClusterInfo());

        System.out.println("\nNode 3 view:");
        System.out.println(node3.getClusterInfo());

        // Keep running
        Thread.currentThread().join();
    }
}
