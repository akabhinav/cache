package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class GetCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'get' command");
        }

        String key = args.get(0);
        return dataStore.get(key)
            .map(value -> {
                if (value instanceof RedisValue.StringValue sv) {
                    try {
                        return RespEncoder.bulkString(sv.value());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElseGet(() -> {
                try {
                    return RespEncoder.nullBulkString();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
    }
}
