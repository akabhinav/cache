package com.distributedcache.redis.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Leader election using Bully Algorithm
 *
 * In the Bully Algorithm:
 * - Each node has a unique ID (we use nodeId)
 * - When a node notices the leader is down, it starts an election
 * - The node with the highest ID becomes the leader
 * - Leader sends periodic "I am leader" messages
 *
 * This is simpler than Raft but sufficient for coordination tasks like:
 * - Cluster state management
 * - Assigning data migration tasks
 * - Coordinating rebalancing
 */
public class LeaderElection {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElection.class);

    private static final long ELECTION_TIMEOUT_MS = 3000;
    private static final long LEADER_HEARTBEAT_INTERVAL_MS = 2000;

    private final ClusterNode localNode;
    private final GossipProtocol gossip;
    private final ScheduledExecutorService scheduler;

    private volatile ClusterNode currentLeader;
    private volatile long currentTerm;
    private volatile boolean electionInProgress;
    private final Set<ClusterNode> electionResponses;
    private final List<LeaderChangeListener> listeners;

    public LeaderElection(ClusterNode localNode, GossipProtocol gossip) {
        this.localNode = localNode;
        this.gossip = gossip;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.currentTerm = 0;
        this.electionInProgress = false;
        this.electionResponses = ConcurrentHashMap.newKeySet();
        this.listeners = new CopyOnWriteArrayList<>();

        // Listen for gossip events
        gossip.addListener(new GossipProtocol.NodeEventListener() {
            @Override
            public void onNodeDead(ClusterNode node) {
                if (node.equals(currentLeader)) {
                    logger.warn("Leader {} is dead, starting election", node);
                    startElection();
                }
            }

            @Override
            public void onLeaderElection(ClusterMessage.LeaderElection election) {
                handleElectionMessage(election);
            }
        });
    }

    /**
     * Start the leader election process
     */
    public void start() {
        // Initial election
        scheduler.schedule(this::startElection, 1000, TimeUnit.MILLISECONDS);

        // Periodic leader check
        scheduler.scheduleAtFixedRate(
            this::checkLeader,
            LEADER_HEARTBEAT_INTERVAL_MS * 2,
            LEADER_HEARTBEAT_INTERVAL_MS * 2,
            TimeUnit.MILLISECONDS
        );

        logger.info("Leader election started");
    }

    /**
     * Stop leader election
     */
    public void stop() {
        scheduler.shutdown();
    }

    /**
     * Get the current leader
     */
    public Optional<ClusterNode> getLeader() {
        return Optional.ofNullable(currentLeader);
    }

    /**
     * Check if this node is the leader
     */
    public boolean isLeader() {
        return localNode.equals(currentLeader);
    }

    /**
     * Get current term
     */
    public long getCurrentTerm() {
        return currentTerm;
    }

    /**
     * Add listener for leader changes
     */
    public void addListener(LeaderChangeListener listener) {
        listeners.add(listener);
    }

    /**
     * Start an election
     */
    public synchronized void startElection() {
        if (electionInProgress) {
            return;
        }

        electionInProgress = true;
        currentTerm++;
        electionResponses.clear();

        logger.info("Starting election for term {}", currentTerm);

        // Get all alive nodes
        Set<ClusterNode> aliveNodes = gossip.getAliveNodes();

        // Find nodes with higher IDs
        List<ClusterNode> higherNodes = aliveNodes.stream()
            .filter(node -> compareNodeIds(node.nodeId(), localNode.nodeId()) > 0)
            .toList();

        if (higherNodes.isEmpty()) {
            // I have the highest ID, I am the leader
            becomeLeader();
        } else {
            // Send election messages to higher nodes
            ClusterMessage.LeaderElection electionMsg = new ClusterMessage.LeaderElection(
                localNode,
                currentTerm,
                System.currentTimeMillis()
            );

            for (ClusterNode node : higherNodes) {
                sendElectionMessage(node, electionMsg);
            }

            // Wait for responses
            scheduler.schedule(() -> {
                if (electionResponses.isEmpty()) {
                    // No responses, become leader
                    becomeLeader();
                } else {
                    // Some higher node responded, wait for it to become leader
                    electionInProgress = false;
                }
            }, ELECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Handle election message from another node
     */
    private void handleElectionMessage(ClusterMessage.LeaderElection election) {
        ClusterNode candidate = election.candidate();
        long term = election.term();

        logger.debug("Received election message from {} for term {}", candidate, term);

        // If candidate has lower ID, reject and start own election
        if (compareNodeIds(localNode.nodeId(), candidate.nodeId()) > 0) {
            // Send rejection (by starting our own election)
            electionResponses.add(localNode);
            startElection();
        } else {
            // Acknowledge the election
            electionResponses.add(candidate);
        }
    }

    /**
     * Become the leader
     */
    private synchronized void becomeLeader() {
        if (currentLeader != null && currentLeader.equals(localNode)) {
            return; // Already leader
        }

        logger.info("Node {} became leader for term {}", localNode, currentTerm);
        currentLeader = localNode;
        electionInProgress = false;

        // Announce leadership
        announceLeadership();

        // Start sending periodic leader heartbeats
        scheduler.scheduleAtFixedRate(
            this::announceLeadership,
            LEADER_HEARTBEAT_INTERVAL_MS,
            LEADER_HEARTBEAT_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );

        // Notify listeners
        notifyLeaderChanged(localNode);
    }

    /**
     * Announce leadership to cluster
     */
    private void announceLeadership() {
        if (!isLeader()) {
            return;
        }

        Map<ClusterNode, NodeState> nodeStates = gossip.getNodeStates();

        ClusterMessage.ClusterStateSync sync = new ClusterMessage.ClusterStateSync(
            localNode,
            nodeStates,
            localNode,
            currentTerm,
            System.currentTimeMillis()
        );

        // This would be sent via gossip to all nodes
        logger.debug("Announcing leadership for term {}", currentTerm);
    }

    /**
     * Check if leader is still alive
     */
    private void checkLeader() {
        if (currentLeader == null) {
            startElection();
            return;
        }

        if (currentLeader.equals(localNode)) {
            return; // We are the leader
        }

        // Check if leader is still alive
        NodeState leaderState = gossip.getNodeStates().get(currentLeader);
        if (leaderState != NodeState.ALIVE) {
            logger.warn("Leader {} is not alive (state: {}), starting election",
                currentLeader, leaderState);
            startElection();
        }
    }

    /**
     * Compare node IDs lexicographically
     */
    private int compareNodeIds(String id1, String id2) {
        return id1.compareTo(id2);
    }

    /**
     * Send election message (would use gossip protocol)
     */
    private void sendElectionMessage(ClusterNode target, ClusterMessage.LeaderElection message) {
        // In a real implementation, this would send via network
        logger.debug("Sending election message to {}", target);
    }

    /**
     * Notify listeners of leader change
     */
    private void notifyLeaderChanged(ClusterNode newLeader) {
        listeners.forEach(l -> l.onLeaderChanged(newLeader, currentTerm));
    }

    /**
     * Listener for leader changes
     */
    public interface LeaderChangeListener {
        void onLeaderChanged(ClusterNode newLeader, long term);
    }
}
