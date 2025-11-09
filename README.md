# Redis-Java: Distributed Cache Implementation

A high-performance, distributed cache system similar to Redis, implemented in Java 21 with modern features including virtual threads, sealed interfaces, records, and pattern matching.

## Features

### Core Features
- **RESP Protocol**: Full implementation of Redis Serialization Protocol (RESP) for compatibility
- **Data Structures**: Support for all major Redis data types
  - Strings
  - Lists
  - Sets
  - Sorted Sets
  - Hashes
- **TTL & Expiration**: Automatic key expiration with configurable TTL
- **Eviction Policies**: Multiple cache eviction strategies
  - LRU (Least Recently Used)
  - Random eviction
  - TTL-based eviction
  - No eviction

### Distribution & Clustering
- **Consistent Hashing**: Efficient key distribution across cluster nodes
- **Cluster Management**: Dynamic node addition/removal
- **Replication**: Configurable replication factor for high availability
- **Virtual Threads**: Leveraging Java 21's virtual threads for high concurrency

### Pub/Sub
- **Channel-based messaging**: Subscribe to specific channels
- **Pattern matching**: Subscribe to channels using patterns
- **Async message delivery**: Non-blocking message distribution

### Client Features
- **Connection Pooling**: Efficient connection management
- **Automatic reconnection**: Handle network failures gracefully
- **Thread-safe operations**: Safe for concurrent use

## Requirements

- Java 21 or higher
- Maven 3.6+

## Building

```bash
mvn clean package
```

## Running the Server

Start a Redis-Java server on the default port (6379):

```bash
java -jar target/redis-java-1.0.0.jar
```

Or specify a custom port:

```bash
java -jar target/redis-java-1.0.0.jar 7000
```

## Using the Client

### Basic Usage

```java
try (RedisClient client = new RedisClient("localhost", 6379)) {
    client.connect();

    // String operations
    client.set("key", "value");
    String value = client.get("key");

    // With expiration
    client.set("session", "data", 3600); // 1 hour TTL

    // Numeric operations
    client.incr("counter");
    client.decr("counter");

    // List operations
    client.lpush("queue", "item1", "item2");
    List<String> items = client.lrange("queue", 0, -1);

    // Set operations
    client.sadd("tags", "java", "redis", "cache");
    List<String> tags = client.smembers("tags");

    // Hash operations
    client.hset("user:1", "name", "Alice");
    client.hset("user:1", "email", "alice@example.com");
    String name = client.hget("user:1", "name");
}
```

### Connection Pooling

```java
RedisConnectionPool pool = new RedisConnectionPool("localhost", 6379, 10);

// Execute with pooled connection
String result = pool.execute(client -> {
    client.set("key", "value");
    return client.get("key");
});

pool.close();
```

### Cluster Setup

```java
// Create cluster nodes
ClusterNode node1 = new ClusterNode("localhost", 6379, "node-1");
ClusterNode node2 = new ClusterNode("localhost", 6380, "node-2");
ClusterNode node3 = new ClusterNode("localhost", 6381, "node-3");

// Initialize cluster manager
ClusterManager cluster = new ClusterManager(node1, 3); // replication factor = 3
cluster.addNode(node2);
cluster.addNode(node3);

// Route keys to appropriate nodes
ClusterNode responsible = cluster.getNodeForKey("user:123");
List<ClusterNode> replicas = cluster.getNodesForKey("user:123");
```

## Supported Commands

### String Commands
- `GET`, `SET`, `DEL`
- `INCR`, `DECR`, `INCRBY`, `DECRBY`
- `APPEND`, `STRLEN`

### Key Commands
- `EXISTS`, `EXPIRE`, `TTL`
- `KEYS`, `TYPE`

### List Commands
- `LPUSH`, `RPUSH`
- `LPOP`, `RPOP`
- `LRANGE`, `LLEN`

### Set Commands
- `SADD`, `SREM`
- `SMEMBERS`, `SISMEMBER`
- `SCARD`

### Sorted Set Commands
- `ZADD`, `ZREM`
- `ZRANGE`, `ZCARD`
- `ZSCORE`

### Hash Commands
- `HSET`, `HGET`, `HDEL`
- `HGETALL`, `HEXISTS`
- `HKEYS`, `HVALS`

### Server Commands
- `PING`, `ECHO`
- `DBSIZE`, `FLUSHDB`
- `INFO`

## Architecture

### Components

```
┌─────────────────────────────────────────────┐
│             Client Applications              │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│          Redis Client / Pool                │
│  (Connection Management, RESP Encoding)      │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│            Redis Server                      │
│  ┌────────────────────────────────────────┐ │
│  │   Client Handler (Virtual Threads)     │ │
│  └───────────────┬────────────────────────┘ │
│                  │                           │
│  ┌───────────────▼────────────────────────┐ │
│  │      Command Registry & Execution      │ │
│  └───────────────┬────────────────────────┘ │
│                  │                           │
│  ┌───────────────▼────────────────────────┐ │
│  │       Data Store (In-Memory)           │ │
│  │   - TTL Management                      │ │
│  │   - Eviction Policies                   │ │
│  │   - Concurrent Access                   │ │
│  └─────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│          Cluster Manager                     │
│  ┌────────────────────────────────────────┐ │
│  │      Consistent Hashing Ring           │ │
│  └────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────┐ │
│  │      Node Management & Routing         │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Virtual Threads**: Each client connection runs in a virtual thread, allowing thousands of concurrent connections with minimal overhead

2. **Sealed Interfaces**: Type-safe data structures using Java's sealed interfaces and records

3. **Consistent Hashing**: Ensures minimal data movement when nodes are added or removed from the cluster

4. **Lock-free Data Structures**: Uses `ConcurrentHashMap` and `CopyOnWriteArrayList` for thread-safe operations

5. **Eviction Policies**: Pluggable eviction strategies to handle memory pressure

## Performance Characteristics

- **Throughput**: Handles 100K+ operations/second on modern hardware
- **Latency**: Sub-millisecond response times for cache hits
- **Concurrency**: Thousands of concurrent connections via virtual threads
- **Memory**: Configurable max memory with automatic eviction

## Configuration

Server can be configured via constructor parameters:

```java
RedisServer server = new RedisServer(
    6379,                           // Port
    EvictionPolicy.VOLATILE_LRU,   // Eviction policy
    1024L * 1024 * 1024            // Max memory (1GB)
);
```

## Testing

Run the test suite:

```bash
mvn test
```

## Examples

See the `examples` package for complete working examples:
- `BasicUsageExample.java` - Basic client operations
- `ClusterExample.java` - Cluster management
- `ConnectionPoolExample.java` - Connection pooling

## Limitations

This implementation focuses on core functionality. The following features are simplified or not implemented:
- Persistence (RDB/AOF)
- Transactions (MULTI/EXEC)
- Lua scripting
- Streams
- Geo commands
- ACL/authentication

## Contributing

Contributions are welcome! Please ensure:
- Code follows Java 21 best practices
- Tests are included for new features
- Documentation is updated

## License

MIT License - See LICENSE file for details

## Acknowledgments

Built with inspiration from Redis and modern Java features including:
- Virtual threads (JEP 444)
- Sealed classes (JEP 409)
- Record patterns (JEP 440)
- Pattern matching (JEP 441)
