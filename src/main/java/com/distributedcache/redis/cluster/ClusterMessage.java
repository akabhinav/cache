package com.distributedcache.redis.cluster;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Messages exchanged between cluster nodes
 */
public sealed interface ClusterMessage extends Serializable permits
    ClusterMessage.Heartbeat,
    ClusterMessage.JoinRequest,
    ClusterMessage.JoinResponse,
    ClusterMessage.NodeUpdate,
    ClusterMessage.DataReplication,
    ClusterMessage.LeaderElection,
    ClusterMessage.ClusterStateSync {

    MessageType type();

    enum MessageType {
        HEARTBEAT,
        JOIN_REQUEST,
        JOIN_RESPONSE,
        NODE_UPDATE,
        DATA_REPLICATION,
        LEADER_ELECTION,
        CLUSTER_STATE_SYNC
    }

    /**
     * Periodic heartbeat to indicate node is alive
     */
    record Heartbeat(
        ClusterNode sender,
        long timestamp,
        Map<String, NodeState> knownNodes
    ) implements ClusterMessage {
        @Override
        public MessageType type() {
            return MessageType.HEARTBEAT;
        }
    }

    /**
     * Request to join the cluster
     */
    record JoinRequest(
        ClusterNode joiningNode,
        long timestamp
    ) implements ClusterMessage {
        @Override
        public MessageType type() {
            return MessageType.JOIN_REQUEST;
        }
    }

    /**
     * Response to join request with current cluster state
     */
    record JoinResponse(
        ClusterNode responder,
        Set<ClusterNode> currentNodes,
        ClusterNode leader,
        long timestamp
    ) implements ClusterMessage {
        @Override
        public MessageType type() {
            return MessageType.JOIN_RESPONSE;
        }
    }

    /**
     * Update about node state changes
     */
    record NodeUpdate(
        ClusterNode sender,
        ClusterNode affectedNode,
        NodeState newState,
        long timestamp
    ) implements ClusterMessage {
        @Override
        public MessageType type() {
            return MessageType.NODE_UPDATE;
        }
    }

    /**
     * Replicate data to other nodes
     */
    record DataReplication(
        ClusterNode sender,
        String key,
        byte[] value,
        long expirationTime,
        long timestamp
    ) implements ClusterMessage {
        @Override
        public MessageType type() {
            return MessageType.DATA_REPLICATION;
        }
    }

    /**
     * Leader election message
     */
    record LeaderElection(
        ClusterNode candidate,
        long term,
        long timestamp
    ) implements ClusterMessage {
        @Override
        public MessageType type() {
            return MessageType.LEADER_ELECTION;
        }
    }

    /**
     * Synchronize cluster state
     */
    record ClusterStateSync(
        ClusterNode sender,
        Map<ClusterNode, NodeState> nodeStates,
        ClusterNode currentLeader,
        long term,
        long timestamp
    ) implements ClusterMessage {
        @Override
        public MessageType type() {
            return MessageType.CLUSTER_STATE_SYNC;
        }
    }
}
