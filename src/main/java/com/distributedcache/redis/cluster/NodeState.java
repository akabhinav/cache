package com.distributedcache.redis.cluster;

/**
 * Represents the state of a node in the cluster
 */
public enum NodeState {
    /**
     * Node is healthy and operational
     */
    ALIVE,

    /**
     * Node is suspected to be down (missed heartbeats)
     */
    SUSPECTED,

    /**
     * Node is confirmed dead
     */
    DEAD,

    /**
     * Node is joining the cluster
     */
    JOINING,

    /**
     * Node is leaving the cluster
     */
    LEAVING
}
