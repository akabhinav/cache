package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.RedisValue;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class SetCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 2) {
            return RespEncoder.error("ERR wrong number of arguments for 'set' command");
        }

        String key = args.get(0);
        String value = args.get(1);

        // Check for optional EX (seconds) or PX (milliseconds) parameters
        if (args.size() >= 4) {
            String option = args.get(2).toUpperCase();
            long ttl = Long.parseLong(args.get(3));

            if ("EX".equals(option)) {
                dataStore.set(key, new RedisValue.StringValue(value), ttl * 1000);
            } else if ("PX".equals(option)) {
                dataStore.set(key, new RedisValue.StringValue(value), ttl);
            } else {
                return RespEncoder.error("ERR syntax error");
            }
        } else {
            dataStore.set(key, new RedisValue.StringValue(value));
        }

        return RespEncoder.ok();
    }
}
