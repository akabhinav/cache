package com.distributedcache.redis.cluster;

import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.EvictionPolicy;
import com.distributedcache.redis.server.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Distributed Redis server with cluster coordination
 *
 * This integrates all the distributed components:
 * - Gossip protocol for node discovery
 * - Leader election for coordination
 * - Data replication for high availability
 * - Cluster management for routing
 */
public class DistributedRedisServer {
    private static final Logger logger = LoggerFactory.getLogger(DistributedRedisServer.class);

    private final ClusterNode localNode;
    private final RedisServer redisServer;
    private final GossipProtocol gossip;
    private final LeaderElection leaderElection;
    private final ClusterManager clusterManager;
    private final ReplicationManager replicationManager;
    private final List<ClusterNode> seedNodes;

    private DistributedRedisServer(Builder builder) {
        this.localNode = new ClusterNode(
            builder.host,
            builder.port,
            builder.nodeId != null ? builder.nodeId : UUID.randomUUID().toString()
        );

        this.seedNodes = builder.seedNodes;

        // Initialize core components
        DataStore dataStore = new DataStore(builder.evictionPolicy, builder.maxMemory);
        this.redisServer = new RedisServer(builder.port, builder.evictionPolicy, builder.maxMemory);

        // Initialize cluster components
        this.gossip = new GossipProtocol(localNode, builder.gossipPort);
        this.clusterManager = new ClusterManager(localNode, builder.replicationFactor);
        this.leaderElection = new LeaderElection(localNode, gossip);
        this.replicationManager = new ReplicationManager(
            localNode,
            dataStore,
            clusterManager,
            gossip,
            builder.replicationFactor
        );

        // Wire up gossip listeners
        gossip.addListener(new GossipProtocol.NodeEventListener() {
            @Override
            public void onNodeJoined(ClusterNode node) {
                logger.info("Node joined cluster: {}", node);
                clusterManager.addNode(node);

                // If we're the leader, trigger data synchronization
                if (leaderElection.isLeader()) {
                    replicationManager.synchronizeCluster();
                }
            }

            @Override
            public void onNodeDead(ClusterNode node) {
                logger.warn("Node left cluster: {}", node);
                clusterManager.removeNode(node);

                // If we're the leader, trigger rebalancing
                if (leaderElection.isLeader()) {
                    replicationManager.synchronizeCluster();
                }
            }
        });

        // Wire up leader election listeners
        leaderElection.addListener((newLeader, term) -> {
            logger.info("New leader elected: {} (term {})", newLeader, term);

            if (newLeader.equals(localNode)) {
                logger.info("This node is now the leader");
                // Leader can coordinate cluster-wide operations
                replicationManager.synchronizeCluster();
            }
        });
    }

    /**
     * Start the distributed server
     */
    public void start() throws IOException {
        logger.info("Starting distributed Redis server: {}", localNode);

        // Start gossip protocol first
        gossip.start();

        // Add seed nodes for bootstrapping
        for (ClusterNode seed : seedNodes) {
            gossip.addSeedNode(seed);
        }

        // Start leader election
        leaderElection.start();

        // Start replication manager
        replicationManager.start();

        // Start Redis server
        redisServer.start();

        logger.info("Distributed Redis server started successfully");
        logger.info("Node ID: {}", localNode.nodeId());
        logger.info("Redis port: {}", localNode.port());
        logger.info("Gossip port: {}", gossip.getClass().getSimpleName());
    }

    /**
     * Stop the distributed server
     */
    public void stop() {
        logger.info("Stopping distributed Redis server");

        redisServer.stop();
        replicationManager.stop();
        leaderElection.stop();
        gossip.stop();

        logger.info("Distributed Redis server stopped");
    }

    /**
     * Get cluster information
     */
    public String getClusterInfo() {
        StringBuilder info = new StringBuilder();
        info.append("=== Distributed Redis Cluster Info ===\n\n");

        info.append("Local Node: ").append(localNode).append("\n");
        info.append("Node State: ALIVE\n\n");

        // Leader info
        Optional<ClusterNode> leader = leaderElection.getLeader();
        info.append("Leader: ").append(leader.map(Object::toString).orElse("NONE"))
            .append(" (term ").append(leaderElection.getCurrentTerm()).append(")\n");
        info.append("Is Leader: ").append(leaderElection.isLeader()).append("\n\n");

        // Cluster topology
        info.append(clusterManager.getClusterInfo()).append("\n");

        // Node states
        info.append("Node States:\n");
        Map<ClusterNode, NodeState> states = gossip.getNodeStates();
        states.forEach((node, state) ->
            info.append("  ").append(node).append(" -> ").append(state).append("\n"));

        info.append("\n");

        // Replication stats
        info.append("Replication: ").append(replicationManager.getStats()).append("\n");

        return info.toString();
    }

    /**
     * Get the local node
     */
    public ClusterNode getLocalNode() {
        return localNode;
    }

    /**
     * Check if this node is the leader
     */
    public boolean isLeader() {
        return leaderElection.isLeader();
    }

    /**
     * Get cluster manager
     */
    public ClusterManager getClusterManager() {
        return clusterManager;
    }

    /**
     * Builder for DistributedRedisServer
     */
    public static class Builder {
        private String host = "localhost";
        private int port = 6379;
        private int gossipPort = 7379;
        private String nodeId;
        private int replicationFactor = 3;
        private EvictionPolicy evictionPolicy = EvictionPolicy.VOLATILE_LRU;
        private long maxMemory = 1024L * 1024 * 1024; // 1GB
        private List<ClusterNode> seedNodes = new ArrayList<>();

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder gossipPort(int gossipPort) {
            this.gossipPort = gossipPort;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder replicationFactor(int replicationFactor) {
            this.replicationFactor = replicationFactor;
            return this;
        }

        public Builder evictionPolicy(EvictionPolicy policy) {
            this.evictionPolicy = policy;
            return this;
        }

        public Builder maxMemory(long maxMemory) {
            this.maxMemory = maxMemory;
            return this;
        }

        public Builder addSeedNode(String host, int port, String nodeId) {
            this.seedNodes.add(new ClusterNode(host, port, nodeId));
            return this;
        }

        public Builder addSeedNode(ClusterNode node) {
            this.seedNodes.add(node);
            return this;
        }

        public DistributedRedisServer build() {
            return new DistributedRedisServer(this);
        }
    }

    /**
     * Main method to run a distributed Redis server
     */
    public static void main(String[] args) {
        int port = 6379;
        int gossipPort = 7379;
        String nodeId = null;
        List<ClusterNode> seeds = new ArrayList<>();

        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--gossip-port" -> gossipPort = Integer.parseInt(args[++i]);
                case "--node-id" -> nodeId = args[++i];
                case "--seed" -> {
                    String[] parts = args[++i].split(":");
                    seeds.add(new ClusterNode(parts[0], Integer.parseInt(parts[1]), parts[2]));
                }
            }
        }

        // Build server
        Builder builder = new Builder()
            .port(port)
            .gossipPort(gossipPort)
            .replicationFactor(3);

        if (nodeId != null) {
            builder.nodeId(nodeId);
        }

        for (ClusterNode seed : seeds) {
            builder.addSeedNode(seed);
        }

        DistributedRedisServer server = builder.build();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            server.stop();
        }));

        // Start server
        try {
            server.start();

            logger.info("\n" + server.getClusterInfo());

            // Keep main thread alive
            Thread.currentThread().join();

        } catch (IOException e) {
            logger.error("Failed to start server", e);
            System.exit(1);
        } catch (InterruptedException e) {
            logger.info("Server interrupted");
            server.stop();
        }
    }
}
