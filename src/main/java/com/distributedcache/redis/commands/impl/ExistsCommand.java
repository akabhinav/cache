package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class ExistsCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'exists' command");
        }

        long count = args.stream()
            .filter(dataStore::exists)
            .count();

        return RespEncoder.integer(count);
    }
}
