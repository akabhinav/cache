package com.distributedcache.redis.core;

/**
 * Eviction policies for cache
 */
public enum EvictionPolicy {
    /**
     * No eviction - return error when memory limit is reached
     */
    NO_EVICTION,

    /**
     * Evict keys with expiration set, least recently used first
     */
    VOLATILE_LRU,

    /**
     * Evict any key, least recently used first
     */
    ALL_KEYS_LRU,

    /**
     * Evict keys with expiration set, random selection
     */
    VOLATILE_RANDOM,

    /**
     * Evict any key, random selection
     */
    ALL_KEYS_RANDOM,

    /**
     * Evict keys with expiration set, shortest TTL first
     */
    VOLATILE_TTL
}
