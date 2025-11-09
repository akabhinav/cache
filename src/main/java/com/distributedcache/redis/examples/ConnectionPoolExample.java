package com.distributedcache.redis.examples;

import com.distributedcache.redis.client.RedisConnectionPool;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating connection pooling
 */
public class ConnectionPoolExample {

    public static void main(String[] args) {
        System.out.println("=== Redis Java - Connection Pool Example ===\n");

        RedisConnectionPool pool = new RedisConnectionPool("localhost", 6379, 5);

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        // Submit multiple concurrent tasks
        for (int i = 0; i < 20; i++) {
            final int taskId = i;
            executor.submit(() -> {
                try {
                    String result = pool.execute(client -> {
                        String key = "task:" + taskId;
                        client.set(key, "value-" + taskId, 30);
                        return client.get(key);
                    });

                    System.out.println("Task " + taskId + " result: " + result);

                } catch (IOException | InterruptedException e) {
                    System.err.println("Task " + taskId + " failed: " + e.getMessage());
                }
            });
        }

        // Wait for completion
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Show pool stats
        System.out.println("\nFinal pool stats: " + pool.getStats());

        pool.close();
    }
}
