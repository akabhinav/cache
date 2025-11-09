package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class HSetCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 3 || args.size() % 2 == 0) {
            return RespEncoder.error("ERR wrong number of arguments for 'hset' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        RedisValue.HashValue hash;
        if (value.isEmpty()) {
            hash = new RedisValue.HashValue();
            dataStore.set(key, hash);
        } else if (value.get() instanceof RedisValue.HashValue hv) {
            hash = hv;
        } else {
            return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        long added = 0;
        for (int i = 1; i < args.size(); i += 2) {
            String field = args.get(i);
            String val = args.get(i + 1);

            if (!hash.fields().containsKey(field)) {
                added++;
            }
            hash.fields().put(field, val);
        }

        return RespEncoder.integer(added);
    }
}
