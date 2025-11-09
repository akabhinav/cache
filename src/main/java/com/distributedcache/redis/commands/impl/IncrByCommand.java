package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class IncrByCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 2) {
            return RespEncoder.error("ERR wrong number of arguments for 'incrby' command");
        }

        String key = args.get(0);
        long increment = Long.parseLong(args.get(1));
        var value = dataStore.get(key);

        long newValue;
        if (value.isEmpty()) {
            newValue = increment;
        } else if (value.get() instanceof RedisValue.StringValue sv) {
            try {
                long current = Long.parseLong(sv.value());
                newValue = current + increment;
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
