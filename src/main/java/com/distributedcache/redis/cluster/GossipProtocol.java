package com.distributedcache.redis.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Gossip protocol for node discovery, failure detection, and state dissemination
 *
 * Uses a simple gossip-based protocol where:
 * - Nodes periodically send heartbeats to random peers
 * - Failed nodes are detected via missing heartbeats
 * - Cluster state changes are gossiped to all nodes
 */
public class GossipProtocol {
    private static final Logger logger = LoggerFactory.getLogger(GossipProtocol.class);

    private static final int GOSSIP_INTERVAL_MS = 1000;
    private static final int HEARTBEAT_TIMEOUT_MS = 5000;
    private static final int SUSPICION_TIMEOUT_MS = 3000;

    private final ClusterNode localNode;
    private final int gossipPort;
    private final Map<ClusterNode, NodeInfo> nodeStates;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService networkExecutor;
    private DatagramSocket socket;
    private volatile boolean running;

    // Callbacks for cluster events
    private final List<NodeEventListener> listeners;

    public GossipProtocol(ClusterNode localNode, int gossipPort) {
        this.localNode = localNode;
        this.gossipPort = gossipPort;
        this.nodeStates = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2);
        this.networkExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.listeners = new CopyOnWriteArrayList<>();
        this.running = false;

        // Add self to node states
        nodeStates.put(localNode, new NodeInfo(NodeState.ALIVE, System.currentTimeMillis()));
    }

    /**
     * Start the gossip protocol
     */
    public void start() throws IOException {
        socket = new DatagramSocket(gossipPort);
        running = true;

        // Start UDP receiver
        networkExecutor.submit(this::receiveMessages);

        // Start periodic gossip
        scheduler.scheduleAtFixedRate(
            this::sendGossip,
            GOSSIP_INTERVAL_MS,
            GOSSIP_INTERVAL_MS,
            TimeUnit.MILLISECONDS
        );

        // Start failure detector
        scheduler.scheduleAtFixedRate(
            this::detectFailures,
            HEARTBEAT_TIMEOUT_MS,
            HEARTBEAT_TIMEOUT_MS,
            TimeUnit.MILLISECONDS
        );

        logger.info("Gossip protocol started on port {}", gossipPort);
    }

    /**
     * Stop the gossip protocol
     */
    public void stop() {
        running = false;

        scheduler.shutdown();
        networkExecutor.shutdown();

        if (socket != null && !socket.isClosed()) {
            socket.close();
        }

        logger.info("Gossip protocol stopped");
    }

    /**
     * Add a seed node to bootstrap cluster membership
     */
    public void addSeedNode(ClusterNode node) {
        if (!node.equals(localNode)) {
            nodeStates.putIfAbsent(node, new NodeInfo(NodeState.ALIVE, System.currentTimeMillis()));
            logger.info("Added seed node: {}", node);
        }
    }

    /**
     * Add listener for node events
     */
    public void addListener(NodeEventListener listener) {
        listeners.add(listener);
    }

    /**
     * Get all known nodes and their states
     */
    public Map<ClusterNode, NodeState> getNodeStates() {
        Map<ClusterNode, NodeState> states = new HashMap<>();
        nodeStates.forEach((node, info) -> states.put(node, info.state));
        return states;
    }

    /**
     * Get all alive nodes
     */
    public Set<ClusterNode> getAliveNodes() {
        Set<ClusterNode> alive = new HashSet<>();
        nodeStates.forEach((node, info) -> {
            if (info.state == NodeState.ALIVE) {
                alive.add(node);
            }
        });
        return alive;
    }

    /**
     * Periodically send gossip messages to random peers
     */
    private void sendGossip() {
        try {
            List<ClusterNode> peers = new ArrayList<>(nodeStates.keySet());
            peers.remove(localNode);

            if (peers.isEmpty()) {
                return;
            }

            // Send to a few random peers
            int fanout = Math.min(3, peers.size());
            Collections.shuffle(peers);

            Map<String, NodeState> knownStates = new HashMap<>();
            nodeStates.forEach((node, info) ->
                knownStates.put(node.nodeId(), info.state));

            ClusterMessage.Heartbeat heartbeat = new ClusterMessage.Heartbeat(
                localNode,
                System.currentTimeMillis(),
                knownStates
            );

            for (int i = 0; i < fanout; i++) {
                ClusterNode peer = peers.get(i);
                sendMessage(peer, heartbeat);
            }

        } catch (Exception e) {
            logger.error("Error sending gossip", e);
        }
    }

    /**
     * Detect failed nodes based on missed heartbeats
     */
    private void detectFailures() {
        long now = System.currentTimeMillis();

        nodeStates.forEach((node, info) -> {
            if (node.equals(localNode)) {
                return; // Skip self
            }

            long timeSinceLastHeartbeat = now - info.lastHeartbeat;

            switch (info.state) {
                case ALIVE -> {
                    if (timeSinceLastHeartbeat > SUSPICION_TIMEOUT_MS) {
                        updateNodeState(node, NodeState.SUSPECTED);
                        logger.warn("Node {} is suspected (no heartbeat for {}ms)",
                            node, timeSinceLastHeartbeat);
                    }
                }
                case SUSPECTED -> {
                    if (timeSinceLastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                        updateNodeState(node, NodeState.DEAD);
                        logger.error("Node {} is dead (no heartbeat for {}ms)",
                            node, timeSinceLastHeartbeat);
                        notifyNodeDead(node);
                    }
                }
                case DEAD -> {
                    // Can be removed after some time
                }
            }
        });
    }

    /**
     * Receive and process messages
     */
    private void receiveMessages() {
        byte[] buffer = new byte[8192];

        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                ClusterMessage message = deserialize(packet.getData(), packet.getLength());
                handleMessage(message, packet.getAddress());

            } catch (SocketException e) {
                if (running) {
                    logger.error("Socket error", e);
                }
            } catch (Exception e) {
                logger.error("Error receiving message", e);
            }
        }
    }

    /**
     * Handle received cluster messages
     */
    private void handleMessage(ClusterMessage message, InetAddress fromAddress) {
        switch (message) {
            case ClusterMessage.Heartbeat hb -> handleHeartbeat(hb);
            case ClusterMessage.JoinRequest jr -> handleJoinRequest(jr, fromAddress);
            case ClusterMessage.JoinResponse jresp -> handleJoinResponse(jresp);
            case ClusterMessage.NodeUpdate nu -> handleNodeUpdate(nu);
            case ClusterMessage.DataReplication dr -> handleDataReplication(dr);
            case ClusterMessage.LeaderElection le -> handleLeaderElection(le);
            case ClusterMessage.ClusterStateSync css -> handleClusterStateSync(css);
        }
    }

    private void handleHeartbeat(ClusterMessage.Heartbeat heartbeat) {
        ClusterNode sender = heartbeat.sender();

        // Update sender's heartbeat
        NodeInfo info = nodeStates.get(sender);
        if (info == null) {
            // New node discovered
            nodeStates.put(sender, new NodeInfo(NodeState.ALIVE, System.currentTimeMillis()));
            logger.info("Discovered new node via gossip: {}", sender);
            notifyNodeJoined(sender);
        } else {
            // Update existing node
            if (info.state == NodeState.SUSPECTED || info.state == NodeState.DEAD) {
                logger.info("Node {} is back alive", sender);
                updateNodeState(sender, NodeState.ALIVE);
            }
            info.lastHeartbeat = System.currentTimeMillis();
        }

        // Merge node states from heartbeat
        heartbeat.knownNodes().forEach((nodeId, state) -> {
            // Find node by ID and update state if needed
            nodeStates.keySet().stream()
                .filter(n -> n.nodeId().equals(nodeId))
                .findFirst()
                .ifPresent(node -> {
                    NodeInfo nodeInfo = nodeStates.get(node);
                    if (nodeInfo != null && nodeInfo.state != state) {
                        logger.debug("Updating node {} state from {} to {} (via gossip)",
                            node, nodeInfo.state, state);
                    }
                });
        });
    }

    private void handleJoinRequest(ClusterMessage.JoinRequest request, InetAddress fromAddress) {
        ClusterNode joiningNode = request.joiningNode();
        logger.info("Received join request from {}", joiningNode);

        // Add to known nodes
        nodeStates.put(joiningNode, new NodeInfo(NodeState.JOINING, System.currentTimeMillis()));

        // Send join response with current cluster state
        ClusterMessage.JoinResponse response = new ClusterMessage.JoinResponse(
            localNode,
            nodeStates.keySet(),
            null, // Leader info would go here
            System.currentTimeMillis()
        );

        sendMessage(joiningNode, response);
        notifyNodeJoined(joiningNode);
    }

    private void handleJoinResponse(ClusterMessage.JoinResponse response) {
        logger.info("Received join response from {}", response.responder());

        // Merge cluster state
        for (ClusterNode node : response.currentNodes()) {
            nodeStates.putIfAbsent(node, new NodeInfo(NodeState.ALIVE, System.currentTimeMillis()));
        }
    }

    private void handleNodeUpdate(ClusterMessage.NodeUpdate update) {
        updateNodeState(update.affectedNode(), update.newState());
    }

    private void handleDataReplication(ClusterMessage.DataReplication replication) {
        // This would be handled by the replication manager
        notifyDataReplication(replication);
    }

    private void handleLeaderElection(ClusterMessage.LeaderElection election) {
        // This would be handled by the leader election manager
        notifyLeaderElection(election);
    }

    private void handleClusterStateSync(ClusterMessage.ClusterStateSync sync) {
        logger.debug("Received cluster state sync from {}", sync.sender());

        // Merge cluster state
        sync.nodeStates().forEach((node, state) -> {
            NodeInfo info = nodeStates.get(node);
            if (info != null) {
                info.state = state;
                info.lastHeartbeat = System.currentTimeMillis();
            } else {
                nodeStates.put(node, new NodeInfo(state, System.currentTimeMillis()));
            }
        });
    }

    /**
     * Update node state and notify listeners
     */
    private void updateNodeState(ClusterNode node, NodeState newState) {
        NodeInfo info = nodeStates.get(node);
        if (info != null) {
            NodeState oldState = info.state;
            info.state = newState;

            if (oldState != newState) {
                notifyNodeStateChanged(node, oldState, newState);
            }
        }
    }

    /**
     * Send a message to a specific node
     */
    private void sendMessage(ClusterNode target, ClusterMessage message) {
        try {
            byte[] data = serialize(message);
            InetAddress address = InetAddress.getByName(target.host());
            DatagramPacket packet = new DatagramPacket(data, data.length, address, gossipPort);
            socket.send(packet);
        } catch (IOException e) {
            logger.error("Error sending message to {}", target, e);
        }
    }

    /**
     * Serialize message to bytes
     */
    private byte[] serialize(ClusterMessage message) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(message);
        }
        return baos.toByteArray();
    }

    /**
     * Deserialize message from bytes
     */
    private ClusterMessage deserialize(byte[] data, int length) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data, 0, length);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (ClusterMessage) ois.readObject();
        }
    }

    // Notification methods
    private void notifyNodeJoined(ClusterNode node) {
        listeners.forEach(l -> l.onNodeJoined(node));
    }

    private void notifyNodeDead(ClusterNode node) {
        listeners.forEach(l -> l.onNodeDead(node));
    }

    private void notifyNodeStateChanged(ClusterNode node, NodeState oldState, NodeState newState) {
        listeners.forEach(l -> l.onNodeStateChanged(node, oldState, newState));
    }

    private void notifyDataReplication(ClusterMessage.DataReplication replication) {
        listeners.forEach(l -> l.onDataReplication(replication));
    }

    private void notifyLeaderElection(ClusterMessage.LeaderElection election) {
        listeners.forEach(l -> l.onLeaderElection(election));
    }

    /**
     * Node information holder
     */
    private static class NodeInfo {
        NodeState state;
        long lastHeartbeat;

        NodeInfo(NodeState state, long lastHeartbeat) {
            this.state = state;
            this.lastHeartbeat = lastHeartbeat;
        }
    }

    /**
     * Listener for cluster events
     */
    public interface NodeEventListener {
        default void onNodeJoined(ClusterNode node) {}
        default void onNodeDead(ClusterNode node) {}
        default void onNodeStateChanged(ClusterNode node, NodeState oldState, NodeState newState) {}
        default void onDataReplication(ClusterMessage.DataReplication replication) {}
        default void onLeaderElection(ClusterMessage.LeaderElection election) {}
    }
}
