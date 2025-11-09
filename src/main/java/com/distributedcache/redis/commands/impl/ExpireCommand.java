package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class ExpireCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.size() < 2) {
            return RespEncoder.error("ERR wrong number of arguments for 'expire' command");
        }

        String key = args.get(0);
        long seconds = Long.parseLong(args.get(1));

        boolean success = dataStore.expire(key, seconds * 1000);
        return RespEncoder.integer(success ? 1 : 0);
    }
}
