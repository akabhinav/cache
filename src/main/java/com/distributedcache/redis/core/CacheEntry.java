package com.distributedcache.redis.core;

/**
 * Represents a cache entry with value and expiration time
 */
public record CacheEntry(RedisValue value, long expirationTime) {

    public CacheEntry(RedisValue value) {
        this(value, -1);
    }

    public boolean isExpired() {
        return expirationTime > 0 && System.currentTimeMillis() > expirationTime;
    }

    public CacheEntry withExpiration(long ttlMillis) {
        return new CacheEntry(value, System.currentTimeMillis() + ttlMillis);
    }

    public long ttl() {
        if (expirationTime < 0) {
            return -1; // No expiration
        }
        long remaining = expirationTime - System.currentTimeMillis();
        return remaining > 0 ? remaining : -2; // -2 means expired
    }
}
