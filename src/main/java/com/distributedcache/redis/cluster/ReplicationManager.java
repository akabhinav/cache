package com.distributedcache.redis.cluster;

import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Manages data replication across cluster nodes
 *
 * Replication Strategy:
 * - Uses consistent hashing to determine which nodes should store each key
 * - Primary node handles writes and replicates to replicas
 * - Replicas can serve reads to distribute load
 * - Async replication for performance (eventual consistency)
 */
public class ReplicationManager {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    private final ClusterNode localNode;
    private final DataStore dataStore;
    private final ClusterManager clusterManager;
    private final GossipProtocol gossip;
    private final ExecutorService replicationExecutor;
    private final int replicationFactor;

    // Track pending replications
    private final BlockingQueue<ReplicationTask> replicationQueue;
    private volatile boolean running;

    public ReplicationManager(
        ClusterNode localNode,
        DataStore dataStore,
        ClusterManager clusterManager,
        GossipProtocol gossip,
        int replicationFactor
    ) {
        this.localNode = localNode;
        this.dataStore = dataStore;
        this.clusterManager = clusterManager;
        this.gossip = gossip;
        this.replicationFactor = replicationFactor;
        this.replicationExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.replicationQueue = new LinkedBlockingQueue<>();
        this.running = false;

        // Listen for incoming replications
        gossip.addListener(new GossipProtocol.NodeEventListener() {
            @Override
            public void onDataReplication(ClusterMessage.DataReplication replication) {
                handleIncomingReplication(replication);
            }
        });
    }

    /**
     * Start the replication manager
     */
    public void start() {
        running = true;

        // Start replication worker
        replicationExecutor.submit(this::processReplicationQueue);

        logger.info("Replication manager started with factor {}", replicationFactor);
    }

    /**
     * Stop the replication manager
     */
    public void stop() {
        running = false;
        replicationExecutor.shutdown();
    }

    /**
     * Replicate a key-value pair to appropriate nodes
     *
     * Called after a write operation on the primary node
     */
    public void replicateWrite(String key, RedisValue value, long expirationTime) {
        // Check if we're the primary node for this key
        ClusterNode primary = clusterManager.getNodeForKey(key);
        if (!primary.equals(localNode)) {
            logger.warn("Attempting to replicate key {} but we're not the primary", key);
            return;
        }

        // Get replica nodes
        List<ClusterNode> replicas = clusterManager.getNodesForKey(key);
        replicas.remove(localNode); // Remove self

        if (replicas.isEmpty()) {
            return;
        }

        // Queue replication task
        ReplicationTask task = new ReplicationTask(key, value, expirationTime, replicas);
        replicationQueue.offer(task);

        logger.debug("Queued replication for key {} to {} replicas", key, replicas.size());
    }

    /**
     * Replicate a key deletion
     */
    public void replicateDelete(String key) {
        ClusterNode primary = clusterManager.getNodeForKey(key);
        if (!primary.equals(localNode)) {
            return;
        }

        List<ClusterNode> replicas = clusterManager.getNodesForKey(key);
        replicas.remove(localNode);

        if (replicas.isEmpty()) {
            return;
        }

        ReplicationTask task = new ReplicationTask(key, null, -1, replicas);
        replicationQueue.offer(task);

        logger.debug("Queued deletion replication for key {} to {} replicas", key, replicas.size());
    }

    /**
     * Handle incoming replication from another node
     */
    private void handleIncomingReplication(ClusterMessage.DataReplication replication) {
        String key = replication.key();
        byte[] valueBytes = replication.value();
        long expirationTime = replication.expirationTime();

        logger.debug("Received replication for key {} from {}", key, replication.sender());

        // Check if we should be a replica for this key
        List<ClusterNode> responsibleNodes = clusterManager.getNodesForKey(key);
        if (!responsibleNodes.contains(localNode)) {
            logger.warn("Received replication for key {} but we're not a replica", key);
            return;
        }

        if (valueBytes == null) {
            // This is a deletion
            dataStore.delete(key);
            logger.debug("Replicated deletion of key {}", key);
        } else {
            // This is a write
            // In a real implementation, we'd deserialize the value properly
            // For now, we'll skip the actual storage since we'd need proper serialization
            logger.debug("Replicated write of key {} with expiration {}", key, expirationTime);
        }
    }

    /**
     * Process replication queue
     */
    private void processReplicationQueue() {
        while (running) {
            try {
                ReplicationTask task = replicationQueue.poll(1, TimeUnit.SECONDS);
                if (task != null) {
                    executeReplication(task);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error processing replication", e);
            }
        }
    }

    /**
     * Execute a replication task
     */
    private void executeReplication(ReplicationTask task) {
        for (ClusterNode replica : task.replicas) {
            try {
                sendReplication(replica, task);
            } catch (Exception e) {
                logger.error("Failed to replicate to {}", replica, e);
                // Could implement retry logic here
            }
        }
    }

    /**
     * Send replication to a specific node
     */
    private void sendReplication(ClusterNode target, ReplicationTask task) {
        // Convert value to bytes (simplified)
        byte[] valueBytes = task.value != null ? serializeValue(task.value) : null;

        ClusterMessage.DataReplication message = new ClusterMessage.DataReplication(
            localNode,
            task.key,
            valueBytes,
            task.expirationTime,
            System.currentTimeMillis()
        );

        // Would send via network in real implementation
        logger.debug("Sending replication to {} for key {}", target, task.key);
    }

    /**
     * Simplified value serialization
     */
    private byte[] serializeValue(RedisValue value) {
        // In a real implementation, use proper serialization
        return new byte[0];
    }

    /**
     * Synchronize data with replicas after node joins/leaves
     *
     * Called by the leader when cluster topology changes
     */
    public void synchronizeCluster() {
        logger.info("Starting cluster data synchronization");

        // Get all keys we're responsible for
        Set<String> keys = dataStore.keys("*");

        for (String key : keys) {
            ClusterNode primary = clusterManager.getNodeForKey(key);

            if (primary.equals(localNode)) {
                // We're the primary, ensure replicas are up to date
                dataStore.get(key).ifPresent(value -> {
                    long ttl = dataStore.ttl(key);
                    long expirationTime = ttl > 0 ? System.currentTimeMillis() + ttl : -1;
                    replicateWrite(key, value, expirationTime);
                });
            }
        }

        logger.info("Cluster synchronization complete");
    }

    /**
     * Get replication lag statistics
     */
    public ReplicationStats getStats() {
        return new ReplicationStats(
            replicationQueue.size(),
            replicationFactor
        );
    }

    /**
     * Replication task
     */
    private record ReplicationTask(
        String key,
        RedisValue value,
        long expirationTime,
        List<ClusterNode> replicas
    ) {}

    /**
     * Replication statistics
     */
    public record ReplicationStats(
        int queueSize,
        int replicationFactor
    ) {
        @Override
        public String toString() {
            return String.format("Replication[queue=%d, factor=%d]",
                queueSize, replicationFactor);
        }
    }
}
