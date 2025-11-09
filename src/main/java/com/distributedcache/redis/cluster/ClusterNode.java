package com.distributedcache.redis.cluster;

import java.util.Objects;

/**
 * Represents a node in the Redis cluster
 */
public record ClusterNode(String host, int port, String nodeId) {

    public ClusterNode {
        Objects.requireNonNull(host, "Host cannot be null");
        Objects.requireNonNull(nodeId, "Node ID cannot be null");
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
    }

    public String getAddress() {
        return host + ":" + port;
    }

    @Override
    public String toString() {
        return nodeId + "@" + getAddress();
    }
}
