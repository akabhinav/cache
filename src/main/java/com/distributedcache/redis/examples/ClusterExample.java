package com.distributedcache.redis.examples;

import com.distributedcache.redis.cluster.ClusterManager;
import com.distributedcache.redis.cluster.ClusterNode;

/**
 * Example demonstrating cluster management
 */
public class ClusterExample {

    public static void main(String[] args) {
        System.out.println("=== Redis Java - Cluster Management Example ===\n");

        // Create local node
        ClusterNode localNode = new ClusterNode("localhost", 6379, "node-1");

        // Create cluster manager
        ClusterManager clusterManager = new ClusterManager(localNode, 3);

        // Add other nodes to the cluster
        clusterManager.addNode(new ClusterNode("localhost", 6380, "node-2"));
        clusterManager.addNode(new ClusterNode("localhost", 6381, "node-3"));
        clusterManager.addNode(new ClusterNode("localhost", 6382, "node-4"));

        // Display cluster info
        System.out.println("Cluster Information:");
        System.out.println(clusterManager.getClusterInfo());

        // Test key routing
        System.out.println("\nKey Routing Examples:");
        String[] testKeys = {"user:1", "user:2", "product:100", "session:abc"};

        for (String key : testKeys) {
            ClusterNode responsible = clusterManager.getNodeForKey(key);
            boolean isLocal = clusterManager.isLocalNodeResponsible(key);

            System.out.printf("Key '%s' -> %s %s\n",
                key,
                responsible,
                isLocal ? "(LOCAL)" : "");
        }

        // Show replication
        System.out.println("\nReplication Examples:");
        for (String key : testKeys) {
            var nodes = clusterManager.getNodesForKey(key);
            System.out.printf("Key '%s' replicated to: %s\n", key, nodes);
        }
    }
}
