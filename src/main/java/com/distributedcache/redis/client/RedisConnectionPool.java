package com.distributedcache.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Connection pool for Redis clients
 */
public class RedisConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(RedisConnectionPool.class);

    private final String host;
    private final int port;
    private final int maxConnections;
    private final BlockingQueue<RedisClient> availableConnections;
    private final BlockingQueue<RedisClient> activeConnections;
    private volatile boolean closed;

    public RedisConnectionPool(String host, int port, int maxConnections) {
        this.host = host;
        this.port = port;
        this.maxConnections = maxConnections;
        this.availableConnections = new LinkedBlockingQueue<>(maxConnections);
        this.activeConnections = new LinkedBlockingQueue<>(maxConnections);
        this.closed = false;

        // Initialize pool
        initializePool();
    }

    public RedisConnectionPool(String host, int port) {
        this(host, port, 10); // Default 10 connections
    }

    private void initializePool() {
        for (int i = 0; i < maxConnections; i++) {
            try {
                RedisClient client = new RedisClient(host, port);
                client.connect();
                availableConnections.offer(client);
            } catch (IOException e) {
                logger.error("Failed to create connection", e);
            }
        }
        logger.info("Initialized connection pool with {} connections", availableConnections.size());
    }

    /**
     * Get a connection from the pool
     */
    public RedisClient getConnection() throws IOException, InterruptedException {
        if (closed) {
            throw new IOException("Connection pool is closed");
        }

        RedisClient client = availableConnections.poll(5, TimeUnit.SECONDS);
        if (client == null) {
            throw new IOException("Failed to get connection from pool: timeout");
        }

        activeConnections.offer(client);
        return client;
    }

    /**
     * Return a connection to the pool
     */
    public void returnConnection(RedisClient client) {
        if (client == null) {
            return;
        }

        activeConnections.remove(client);

        if (closed) {
            client.close();
        } else {
            availableConnections.offer(client);
        }
    }

    /**
     * Execute an operation using a pooled connection
     */
    public <T> T execute(PooledOperation<T> operation) throws IOException, InterruptedException {
        RedisClient client = getConnection();
        try {
            return operation.execute(client);
        } finally {
            returnConnection(client);
        }
    }

    /**
     * Close all connections in the pool
     */
    public void close() {
        closed = true;

        // Close available connections
        RedisClient client;
        while ((client = availableConnections.poll()) != null) {
            client.close();
        }

        // Close active connections
        while ((client = activeConnections.poll()) != null) {
            client.close();
        }

        logger.info("Connection pool closed");
    }

    /**
     * Get pool statistics
     */
    public PoolStats getStats() {
        return new PoolStats(
            availableConnections.size(),
            activeConnections.size(),
            maxConnections
        );
    }

    @FunctionalInterface
    public interface PooledOperation<T> {
        T execute(RedisClient client) throws IOException;
    }

    public record PoolStats(int available, int active, int total) {
        @Override
        public String toString() {
            return String.format("Pool[available=%d, active=%d, total=%d]", available, active, total);
        }
    }
}
