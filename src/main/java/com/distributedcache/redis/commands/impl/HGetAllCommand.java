package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;
import com.distributedcache.redis.protocol.RespValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HGetAllCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'hgetall' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        if (value.isEmpty()) {
            return RespEncoder.array(new ArrayList<>());
        }

        if (value.get() instanceof RedisValue.HashValue hv) {
            List<String> result = new ArrayList<>();
            for (Map.Entry<String, String> entry : hv.fields().entrySet()) {
                result.add(entry.getKey());
                result.add(entry.getValue());
            }
            return RespEncoder.array(result);
        }

        return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
