package com.distributedcache.redis.client;

import com.distributedcache.redis.protocol.RespEncoder;
import com.distributedcache.redis.protocol.RespParser;
import com.distributedcache.redis.protocol.RespValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Redis client implementation
 */
public class RedisClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RedisClient.class);

    private final String host;
    private final int port;
    private Socket socket;
    private OutputStream out;
    private InputStream in;
    private RespParser parser;

    public RedisClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public RedisClient() {
        this("localhost", 6379);
    }

    /**
     * Connect to the Redis server
     */
    public void connect() throws IOException {
        socket = new Socket(host, port);
        out = socket.getOutputStream();
        in = socket.getInputStream();
        parser = new RespParser(in);
        logger.info("Connected to Redis server at {}:{}", host, port);
    }

    /**
     * Execute a command and return the response
     */
    public RespValue execute(String... args) throws IOException {
        if (socket == null || socket.isClosed()) {
            throw new IOException("Not connected to server");
        }

        // Build command array
        List<RespValue> elements = new ArrayList<>();
        for (String arg : args) {
            elements.add(new RespValue.BulkString(arg));
        }

        RespValue command = new RespValue.Array(elements);
        byte[] encoded = RespEncoder.encode(command);

        // Send command
        out.write(encoded);
        out.flush();

        // Read response
        return parser.parse();
    }

    /**
     * Execute a command and return string result
     */
    public String executeString(String... args) throws IOException {
        RespValue response = execute(args);

        return switch (response) {
            case RespValue.SimpleString s -> s.value();
            case RespValue.BulkString b -> b.isNull() ? null : b.value();
            case RespValue.Integer i -> String.valueOf(i.value());
            case RespValue.Error e -> throw new IOException("Redis error: " + e.message());
            case RespValue.Array a -> a.isNull() ? null : a.values().toString();
        };
    }

    /**
     * SET command
     */
    public String set(String key, String value) throws IOException {
        return executeString("SET", key, value);
    }

    /**
     * SET command with expiration
     */
    public String set(String key, String value, long ttlSeconds) throws IOException {
        return executeString("SET", key, value, "EX", String.valueOf(ttlSeconds));
    }

    /**
     * GET command
     */
    public String get(String key) throws IOException {
        return executeString("GET", key);
    }

    /**
     * DEL command
     */
    public long del(String... keys) throws IOException {
        RespValue response = execute(concatenate("DEL", keys));
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        return 0;
    }

    /**
     * EXISTS command
     */
    public boolean exists(String key) throws IOException {
        RespValue response = execute("EXISTS", key);
        if (response instanceof RespValue.Integer i) {
            return i.value() > 0;
        }
        return false;
    }

    /**
     * EXPIRE command
     */
    public boolean expire(String key, long seconds) throws IOException {
        RespValue response = execute("EXPIRE", key, String.valueOf(seconds));
        if (response instanceof RespValue.Integer i) {
            return i.value() == 1;
        }
        return false;
    }

    /**
     * TTL command
     */
    public long ttl(String key) throws IOException {
        RespValue response = execute("TTL", key);
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        return -2;
    }

    /**
     * INCR command
     */
    public long incr(String key) throws IOException {
        RespValue response = execute("INCR", key);
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        throw new IOException("Unexpected response type");
    }

    /**
     * DECR command
     */
    public long decr(String key) throws IOException {
        RespValue response = execute("DECR", key);
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        throw new IOException("Unexpected response type");
    }

    /**
     * LPUSH command
     */
    public long lpush(String key, String... values) throws IOException {
        RespValue response = execute(concatenate("LPUSH", key, values));
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        return 0;
    }

    /**
     * RPUSH command
     */
    public long rpush(String key, String... values) throws IOException {
        RespValue response = execute(concatenate("RPUSH", key, values));
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        return 0;
    }

    /**
     * LRANGE command
     */
    public List<String> lrange(String key, int start, int stop) throws IOException {
        RespValue response = execute("LRANGE", key, String.valueOf(start), String.valueOf(stop));
        return extractList(response);
    }

    /**
     * SADD command
     */
    public long sadd(String key, String... members) throws IOException {
        RespValue response = execute(concatenate("SADD", key, members));
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        return 0;
    }

    /**
     * SMEMBERS command
     */
    public List<String> smembers(String key) throws IOException {
        RespValue response = execute("SMEMBERS", key);
        return extractList(response);
    }

    /**
     * HSET command
     */
    public long hset(String key, String field, String value) throws IOException {
        RespValue response = execute("HSET", key, field, value);
        if (response instanceof RespValue.Integer i) {
            return i.value();
        }
        return 0;
    }

    /**
     * HGET command
     */
    public String hget(String key, String field) throws IOException {
        return executeString("HGET", key, field);
    }

    /**
     * PING command
     */
    public String ping() throws IOException {
        return executeString("PING");
    }

    @Override
    public void close() {
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
                logger.info("Disconnected from Redis server");
            }
        } catch (IOException e) {
            logger.error("Error closing connection", e);
        }
    }

    // Helper methods
    private String[] concatenate(String first, String... rest) {
        String[] result = new String[rest.length + 1];
        result[0] = first;
        System.arraycopy(rest, 0, result, 1, rest.length);
        return result;
    }

    private String[] concatenate(String first, String second, String... rest) {
        String[] result = new String[rest.length + 2];
        result[0] = first;
        result[1] = second;
        System.arraycopy(rest, 0, result, 2, rest.length);
        return result;
    }

    private List<String> extractList(RespValue response) {
        if (response instanceof RespValue.Array array && !array.isNull()) {
            List<String> result = new ArrayList<>();
            for (RespValue value : array.values()) {
                if (value instanceof RespValue.BulkString bs && !bs.isNull()) {
                    result.add(bs.value());
                }
            }
            return result;
        }
        return new ArrayList<>();
    }
}
