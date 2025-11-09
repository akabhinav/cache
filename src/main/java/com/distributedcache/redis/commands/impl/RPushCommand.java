package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class RPushCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 2) {
            return RespEncoder.error("ERR wrong number of arguments for 'rpush' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        RedisValue.ListValue list;
        if (value.isEmpty()) {
            list = new RedisValue.ListValue();
            dataStore.set(key, list);
        } else if (value.get() instanceof RedisValue.ListValue lv) {
            list = lv;
        } else {
            return RespEncoder.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        for (int i = 1; i < args.size(); i++) {
            list.values().add(args.get(i));
        }

        return RespEncoder.integer(list.values().size());
    }
}
