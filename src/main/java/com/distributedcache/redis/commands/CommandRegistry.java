package com.distributedcache.redis.commands;

import com.distributedcache.redis.commands.impl.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for all supported commands
 */
public class CommandRegistry {
    private static final Map<String, Command> commands = new HashMap<>();

    static {
        // String commands
        commands.put("GET", new GetCommand());
        commands.put("SET", new SetCommand());
        commands.put("APPEND", new AppendCommand());
        commands.put("STRLEN", new StrlenCommand());
        commands.put("INCR", new IncrCommand());
        commands.put("DECR", new DecrCommand());
        commands.put("INCRBY", new IncrByCommand());
        commands.put("DECRBY", new DecrByCommand());

        // Key commands
        commands.put("DEL", new DelCommand());
        commands.put("EXISTS", new ExistsCommand());
        commands.put("EXPIRE", new ExpireCommand());
        commands.put("TTL", new TtlCommand());
        commands.put("KEYS", new KeysCommand());
        commands.put("TYPE", new TypeCommand());

        // List commands
        commands.put("LPUSH", new LPushCommand());
        commands.put("RPUSH", new RPushCommand());
        commands.put("LPOP", new LPopCommand());
        commands.put("RPOP", new RPopCommand());
        commands.put("LRANGE", new LRangeCommand());
        commands.put("LLEN", new LLenCommand());

        // Set commands
        commands.put("SADD", new SAddCommand());
        commands.put("SREM", new SRemCommand());
        commands.put("SMEMBERS", new SMembersCommand());
        commands.put("SISMEMBER", new SIsMemberCommand());
        commands.put("SCARD", new SCardCommand());

        // Sorted Set commands
        commands.put("ZADD", new ZAddCommand());
        commands.put("ZREM", new ZRemCommand());
        commands.put("ZRANGE", new ZRangeCommand());
        commands.put("ZCARD", new ZCardCommand());
        commands.put("ZSCORE", new ZScoreCommand());

        // Hash commands
        commands.put("HSET", new HSetCommand());
        commands.put("HGET", new HGetCommand());
        commands.put("HDEL", new HDelCommand());
        commands.put("HGETALL", new HGetAllCommand());
        commands.put("HEXISTS", new HExistsCommand());
        commands.put("HKEYS", new HKeysCommand());
        commands.put("HVALS", new HValsCommand());

        // Server commands
        commands.put("PING", new PingCommand());
        commands.put("ECHO", new EchoCommand());
        commands.put("DBSIZE", new DbSizeCommand());
        commands.put("FLUSHDB", new FlushDbCommand());
        commands.put("INFO", new InfoCommand());
    }

    public static Command get(String commandName) {
        return commands.get(commandName.toUpperCase());
    }

    public static boolean exists(String commandName) {
        return commands.containsKey(commandName.toUpperCase());
    }
}
