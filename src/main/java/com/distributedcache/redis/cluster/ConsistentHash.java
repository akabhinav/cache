package com.distributedcache.redis.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Consistent hashing implementation for distributed caching
 */
public class ConsistentHash<T> {
    private final int virtualNodes;
    private final NavigableMap<Long, T> ring;
    private final MessageDigest md;

    public ConsistentHash(int virtualNodes) {
        this.virtualNodes = virtualNodes;
        this.ring = new ConcurrentSkipListMap<>();
        try {
            this.md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    public ConsistentHash() {
        this(150); // Default virtual nodes per physical node
    }

    /**
     * Add a node to the ring
     */
    public void addNode(T node) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(node.toString() + "-" + i);
            ring.put(hash, node);
        }
    }

    /**
     * Remove a node from the ring
     */
    public void removeNode(T node) {
        for (int i = 0; i < virtualNodes; i++) {
            long hash = hash(node.toString() + "-" + i);
            ring.remove(hash);
        }
    }

    /**
     * Get the node responsible for a given key
     */
    public T getNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }

        long hash = hash(key);

        // Find the first node with hash >= key hash
        Map.Entry<Long, T> entry = ring.ceilingEntry(hash);

        // If not found, wrap around to the first node
        if (entry == null) {
            entry = ring.firstEntry();
        }

        return entry.getValue();
    }

    /**
     * Get N nodes responsible for a given key (for replication)
     */
    public List<T> getNodes(String key, int count) {
        if (ring.isEmpty()) {
            return Collections.emptyList();
        }

        Set<T> nodes = new LinkedHashSet<>();
        long hash = hash(key);

        // Get nodes starting from the hash position
        NavigableMap<Long, T> tailMap = ring.tailMap(hash, true);
        for (T node : tailMap.values()) {
            nodes.add(node);
            if (nodes.size() >= count) {
                break;
            }
        }

        // Wrap around if needed
        if (nodes.size() < count) {
            for (T node : ring.values()) {
                nodes.add(node);
                if (nodes.size() >= count) {
                    break;
                }
            }
        }

        return new ArrayList<>(nodes);
    }

    /**
     * Get all nodes in the ring
     */
    public Set<T> getAllNodes() {
        return new HashSet<>(ring.values());
    }

    /**
     * Get the number of nodes in the ring
     */
    public int size() {
        return getAllNodes().size();
    }

    /**
     * Hash function using MD5
     */
    private long hash(String key) {
        md.reset();
        byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));

        // Use the first 8 bytes for the hash
        long hash = 0;
        for (int i = 0; i < 8; i++) {
            hash = (hash << 8) | (digest[i] & 0xFF);
        }

        return hash;
    }
}
