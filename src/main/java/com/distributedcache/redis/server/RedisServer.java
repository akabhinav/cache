package com.distributedcache.redis.server;

import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.EvictionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Main Redis server implementation using Java 21 virtual threads
 */
public class RedisServer {
    private static final Logger logger = LoggerFactory.getLogger(RedisServer.class);

    private final int port;
    private final DataStore dataStore;
    private final ExecutorService executorService;
    private ServerSocket serverSocket;
    private volatile boolean running;

    public RedisServer(int port, EvictionPolicy evictionPolicy, long maxMemoryBytes) {
        this.port = port;
        this.dataStore = new DataStore(evictionPolicy, maxMemoryBytes);
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.running = false;
    }

    public RedisServer(int port) {
        this(port, EvictionPolicy.VOLATILE_LRU, 1024L * 1024 * 1024); // 1GB default
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;

        logger.info("Redis server started on port {}", port);
        logger.info("Using eviction policy: {}", dataStore.getClass().getSimpleName());

        // Accept connections in a separate virtual thread
        Thread.ofVirtual().start(() -> {
            while (running && !serverSocket.isClosed()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    executorService.submit(new ClientHandler(clientSocket, dataStore));
                } catch (IOException e) {
                    if (running) {
                        logger.error("Error accepting client connection", e);
                    }
                }
            }
        });
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
            executorService.shutdown();
            dataStore.shutdown();
            logger.info("Redis server stopped");
        } catch (IOException e) {
            logger.error("Error stopping server", e);
        }
    }

    public boolean isRunning() {
        return running;
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    public static void main(String[] args) {
        int port = 6379; // Default Redis port

        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0]);
                System.exit(1);
            }
        }

        RedisServer server = new RedisServer(port);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down server...");
            server.stop();
        }));

        try {
            server.start();
            logger.info("Server is ready to accept connections");

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
