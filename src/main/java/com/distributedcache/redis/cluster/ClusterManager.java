package com.distributedcache.redis.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Manages cluster membership and routing
 */
public class ClusterManager {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private final ClusterNode localNode;
    private final ConsistentHash<ClusterNode> consistentHash;
    private final Set<ClusterNode> clusterNodes;
    private final Map<String, Set<ClusterNode>> replicationMap;
    private final int replicationFactor;

    public ClusterManager(ClusterNode localNode, int replicationFactor) {
        this.localNode = localNode;
        this.replicationFactor = replicationFactor;
        this.consistentHash = new ConsistentHash<>();
        this.clusterNodes = new CopyOnWriteArraySet<>();
        this.replicationMap = new ConcurrentHashMap<>();

        // Add local node
        addNode(localNode);
    }

    public ClusterManager(ClusterNode localNode) {
        this(localNode, 2); // Default replication factor
    }

    /**
     * Add a node to the cluster
     */
    public void addNode(ClusterNode node) {
        if (clusterNodes.add(node)) {
            consistentHash.addNode(node);
            logger.info("Added node to cluster: {}", node);
            rebalanceReplication();
        }
    }

    /**
     * Remove a node from the cluster
     */
    public void removeNode(ClusterNode node) {
        if (clusterNodes.remove(node)) {
            consistentHash.removeNode(node);
            logger.info("Removed node from cluster: {}", node);
            rebalanceReplication();
        }
    }

    /**
     * Get the primary node responsible for a key
     */
    public ClusterNode getNodeForKey(String key) {
        return consistentHash.getNode(key);
    }

    /**
     * Get all nodes that should store a key (primary + replicas)
     */
    public List<ClusterNode> getNodesForKey(String key) {
        return consistentHash.getNodes(key, Math.min(replicationFactor, clusterNodes.size()));
    }

    /**
     * Check if the local node is responsible for a key
     */
    public boolean isLocalNodeResponsible(String key) {
        ClusterNode responsible = getNodeForKey(key);
        return localNode.equals(responsible);
    }

    /**
     * Check if the local node should replicate a key
     */
    public boolean shouldLocalNodeReplicate(String key) {
        List<ClusterNode> nodes = getNodesForKey(key);
        return nodes.contains(localNode);
    }

    /**
     * Get all nodes in the cluster
     */
    public Set<ClusterNode> getAllNodes() {
        return new HashSet<>(clusterNodes);
    }

    /**
     * Get the local node
     */
    public ClusterNode getLocalNode() {
        return localNode;
    }

    /**
     * Get cluster size
     */
    public int getClusterSize() {
        return clusterNodes.size();
    }

    /**
     * Rebalance replication mapping
     */
    private void rebalanceReplication() {
        replicationMap.clear();

        // For each key range, determine which nodes should replicate
        // This is a simplified version - in production, you'd track actual keys
        logger.debug("Rebalanced replication across {} nodes", clusterNodes.size());
    }

    /**
     * Get cluster info as a string
     */
    public String getClusterInfo() {
        StringBuilder info = new StringBuilder();
        info.append("Cluster size: ").append(clusterNodes.size()).append("\n");
        info.append("Replication factor: ").append(replicationFactor).append("\n");
        info.append("Local node: ").append(localNode).append("\n");
        info.append("\nNodes:\n");

        for (ClusterNode node : clusterNodes) {
            info.append("  - ").append(node);
            if (node.equals(localNode)) {
                info.append(" (local)");
            }
            info.append("\n");
        }

        return info.toString();
    }
}
