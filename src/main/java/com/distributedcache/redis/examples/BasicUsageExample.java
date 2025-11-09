package com.distributedcache.redis.examples;

import com.distributedcache.redis.client.RedisClient;

import java.io.IOException;
import java.util.List;

/**
 * Basic usage examples for Redis client
 */
public class BasicUsageExample {

    public static void main(String[] args) {
        System.out.println("=== Redis Java Client - Basic Usage Examples ===\n");

        try (RedisClient client = new RedisClient("localhost", 6379)) {
            client.connect();

            // Ping server
            System.out.println("1. PING: " + client.ping());

            // String operations
            System.out.println("\n2. String Operations:");
            client.set("name", "Alice");
            System.out.println("   GET name: " + client.get("name"));

            client.set("counter", "0");
            System.out.println("   INCR counter: " + client.incr("counter"));
            System.out.println("   INCR counter: " + client.incr("counter"));
            System.out.println("   DECR counter: " + client.decr("counter"));

            // Expiration
            System.out.println("\n3. Expiration:");
            client.set("temp", "value", 10); // 10 seconds TTL
            System.out.println("   TTL temp: " + client.ttl("temp") + " seconds");
            System.out.println("   EXISTS temp: " + client.exists("temp"));

            // List operations
            System.out.println("\n4. List Operations:");
            client.lpush("tasks", "task1", "task2", "task3");
            client.rpush("tasks", "task4");
            List<String> tasks = client.lrange("tasks", 0, -1);
            System.out.println("   LRANGE tasks: " + tasks);

            // Set operations
            System.out.println("\n5. Set Operations:");
            client.sadd("tags", "java", "redis", "cache", "distributed");
            List<String> tags = client.smembers("tags");
            System.out.println("   SMEMBERS tags: " + tags);

            // Hash operations
            System.out.println("\n6. Hash Operations:");
            client.hset("user:1", "name", "Bob");
            client.hset("user:1", "email", "bob@example.com");
            System.out.println("   HGET user:1 name: " + client.hget("user:1", "name"));
            System.out.println("   HGET user:1 email: " + client.hget("user:1", "email"));

            // Cleanup
            System.out.println("\n7. Cleanup:");
            long deleted = client.del("name", "counter", "temp", "tasks", "tags");
            System.out.println("   Deleted " + deleted + " keys");

        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
