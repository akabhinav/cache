package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class ZCardCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'zcard' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        if (value.isEmpty()) {
            return RespEncoder.integer(0);
        }

        if (value.get() instanceof RedisValue.SortedSetValue ssv) {
            return RespEncoder.integer(ssv.values().size());
        }

        return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
