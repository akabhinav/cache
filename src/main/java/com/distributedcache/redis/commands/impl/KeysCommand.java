package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class KeysCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'keys' command");
        }

        String pattern = args.get(0);
        Set<String> keys = dataStore.keys(pattern);

        return RespEncoder.array(keys.stream().sorted().toList());
    }
}
