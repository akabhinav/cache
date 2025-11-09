package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class SAddCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 2) {
            return RespEncoder.error("ERR wrong number of arguments for 'sadd' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        RedisValue.SetValue set;
        if (value.isEmpty()) {
            set = new RedisValue.SetValue();
            dataStore.set(key, set);
        } else if (value.get() instanceof RedisValue.SetValue sv) {
            set = sv;
        } else {
            return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        long added = 0;
        for (int i = 1; i < args.size(); i++) {
            if (set.values().add(args.get(i))) {
                added++;
            }
        }

        return RespEncoder.integer(added);
    }
}
