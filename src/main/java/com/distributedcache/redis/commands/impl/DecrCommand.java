package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class DecrCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'decr' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        long newValue;
        if (value.isEmpty()) {
            newValue = -1;
        } else if (value.get() instanceof RedisValue.StringValue sv) {
            try {
                long current = Long.parseLong(sv.value());
                newValue = current - 1;
            } catch (NumberFormatException e) {
                return RespEncoder.error("ERR value is not an integer or out of range");
            }
        } else {
            return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        dataStore.set(key, new RedisValue.StringValue(String.valueOf(newValue)));
        return RespEncoder.integer(newValue);
    }
}
