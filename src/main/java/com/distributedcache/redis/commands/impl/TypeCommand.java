package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class TypeCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        if (args.isEmpty()) {
            return RespEncoder.error("ERR wrong number of arguments for 'type' command");
        }

        String key = args.get(0);
        var value = dataStore.get(key);

        if (value.isEmpty()) {
            return RespEncoder.bulkString("none");
        }

        String type = switch (value.get().type()) {
            case STRING -> "string";
            case LIST -> "list";
            case SET -> "set";
            case SORTED_SET -> "zset";
            case HASH -> "hash";
        };

        return RespEncoder.bulkString(type);
    }
}
