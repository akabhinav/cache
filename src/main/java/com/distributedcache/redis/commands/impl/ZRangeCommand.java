package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ZRangeCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 3) {
            return RespEncoder.error("ERR wrong number of arguments for 'zrange' command");
        }

        String key = args.get(0);
        int start = Integer.parseInt(args.get(1));
        int stop = Integer.parseInt(args.get(2));

        var value = dataStore.get(key);

        if (value.isEmpty()) {
            return RespEncoder.array(new ArrayList<>());
        }

        if (value.get() instanceof RedisValue.SortedSetValue ssv) {
            List<Map.Entry<String, Double>> sorted = ssv.getSorted();
            int size = sorted.size();

            // Handle negative indices
            if (start < 0) start = Math.max(0, size + start);
            if (stop < 0) stop = size + stop;

            // Ensure bounds
            start = Math.max(0, Math.min(start, size));
            stop = Math.max(-1, Math.min(stop, size - 1));

            if (start > stop || start >= size) {
                return RespEncoder.array(new ArrayList<>());
            }

            List<String> result = sorted.subList(start, stop + 1).stream()
                .map(Map.Entry::getKey)
                .toList();

            return RespEncoder.array(result);
        }

        return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
