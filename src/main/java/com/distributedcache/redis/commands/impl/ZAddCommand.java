package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class ZAddCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 3 || args.size() % 2 == 0) {
            return RespEncoder.error("ERR wrong number of arguments for 'zadd' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        RedisValue.SortedSetValue sortedSet;
        if (value.isEmpty()) {
            sortedSet = new RedisValue.SortedSetValue();
            dataStore.set(key, sortedSet);
        } else if (value.get() instanceof RedisValue.SortedSetValue ssv) {
            sortedSet = ssv;
        } else {
            return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        long added = 0;
        for (int i = 1; i < args.size(); i += 2) {
            double score = Double.parseDouble(args.get(i));
            String member = args.get(i + 1);

            if (!sortedSet.values().containsKey(member)) {
                added++;
            }
            sortedSet.values().put(member, score);
        }

        return RespEncoder.integer(added);
    }
}
