package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class SIsMemberCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 2) {
            return RespEncoder.error("ERR wrong number of arguments for 'sismember' command");
        }

        String key = args.get(0);
        String member = args.get(1);
        var value = dataStore.get(key);

        if (value.isEmpty()) {
            return RespEncoder.integer(0);
        }

        if (value.get() instanceof RedisValue.SetValue sv) {
            return RespEncoder.integer(sv.values().contains(member) ? 1 : 0);
        }

        return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }
}
