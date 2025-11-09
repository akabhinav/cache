package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class AppendCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 2) {
            return RespEncoder.error("ERR wrong number of arguments for 'append' command");
        }

        String key = args.get(0);
        String appendValue = args.get(1);
        var existing = dataStore.get(key);

        String newValue;
        if (existing.isEmpty()) {
            newValue = appendValue;
        } else if (existing.get() instanceof RedisValue.StringValue sv) {
            newValue = sv.value() + appendValue;
        } else {
            return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        dataStore.set(key, new RedisValue.StringValue(newValue));
        return RespEncoder.integer(newValue.length());
    }
}
