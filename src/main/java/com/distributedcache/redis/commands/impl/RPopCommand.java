package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class RPopCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'rpop' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        if (value.isEmpty()) {
            return RespEncoder.nullBulkString();
        }

        if (value.get() instanceof RedisValue.ListValue lv) {
            if (lv.values().isEmpty()) {
                return RespEncoder.nullBulkString();
            }
            String popped = lv.values().remove(lv.values().size() - 1);
            return RespEncoder.bulkString(popped);
        }

        return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
