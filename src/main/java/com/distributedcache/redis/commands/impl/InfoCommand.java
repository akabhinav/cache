package com.distributedcache.redis.commands.impl;

import com.distributedcache.redis.commands.Command;
import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.protocol.RespEncoder;

import java.io.IOException;
import java.util.List;

public class InfoCommand implements Command {
    @Override
    public byte[] execute(DataStore dataStore, List<String> args) throws IOException {
        StringBuilder info = new StringBuilder();
        info.append("# Server\n");
        info.append("redis_version:7.0.0-java\n");
        info.append("redis_mode:standalone\n");
        info.append("os:").append(System.getProperty("os.name")).append("\n");
        info.append("arch_bits:64\n");
        info.append("\n# Keyspace\n");
        info.append("db0:keys=").append(dataStore.size()).append("\n");

        return RespEncoder.bulkString(info.toString());
    }
}
