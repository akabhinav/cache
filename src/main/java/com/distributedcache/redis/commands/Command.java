package com.distributedcache.redis.commands;

import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.protocol.RespValue;

import java.io.IOException;
import java.util.List;

/**
 * Interface for Redis commands
 */
public interface Command {
    byte[] execute(DataStore dataStore, List<String> args) throws IOException;
}
