package com.distributedcache.redis.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Main data storage engine with TTL support and eviction policies
 */
public class DataStore {
    private static final Logger logger = LoggerFactory.getLogger(DataStore.class);

    private final ConcurrentHashMap<String, CacheEntry> store;
    private final ConcurrentHashMap<String, Long> accessTimes; // For LRU
    private final ScheduledExecutorService expirationExecutor;
    private final ReadWriteLock lock;
    private final EvictionPolicy evictionPolicy;
    private final long maxMemoryBytes;

    public DataStore(EvictionPolicy evictionPolicy, long maxMemoryBytes) {
        this.store = new ConcurrentHashMap<>();
        this.accessTimes = new ConcurrentHashMap<>();
        this.expirationExecutor = Executors.newSingleThreadScheduledExecutor();
        this.lock = new ReentrantReadWriteLock();
        this.evictionPolicy = evictionPolicy;
        this.maxMemoryBytes = maxMemoryBytes;

        // Start expiration cleanup task
        startExpirationCleanup();
    }

    public DataStore() {
        this(EvictionPolicy.VOLATILE_LRU, 1024 * 1024 * 1024); // 1GB default
    }

    private void startExpirationCleanup() {
        expirationExecutor.scheduleAtFixedRate(() -> {
            try {
                cleanupExpiredKeys();
            } catch (Exception e) {
                logger.error("Error during expiration cleanup", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private void cleanupExpiredKeys() {
        List<String> expiredKeys = new ArrayList<>();

        store.forEach((key, entry) -> {
            if (entry.isExpired()) {
                expiredKeys.add(key);
            }
        });

        expiredKeys.forEach(this::delete);

        if (!expiredKeys.isEmpty()) {
            logger.debug("Cleaned up {} expired keys", expiredKeys.size());
        }
    }

    public void set(String key, RedisValue value) {
        lock.writeLock().lock();
        try {
            evictIfNeeded();
            store.put(key, new CacheEntry(value));
            accessTimes.put(key, System.currentTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void set(String key, RedisValue value, long ttlMillis) {
        lock.writeLock().lock();
        try {
            evictIfNeeded();
            store.put(key, new CacheEntry(value).withExpiration(ttlMillis));
            accessTimes.put(key, System.currentTimeMillis());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<RedisValue> get(String key) {
        lock.readLock().lock();
        try {
            CacheEntry entry = store.get(key);
            if (entry == null) {
                return Optional.empty();
            }

            if (entry.isExpired()) {
                // Remove expired entry
                delete(key);
                return Optional.empty();
            }

            accessTimes.put(key, System.currentTimeMillis());
            return Optional.of(entry.value());
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean delete(String key) {
        lock.writeLock().lock();
        try {
            accessTimes.remove(key);
            return store.remove(key) != null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean exists(String key) {
        lock.readLock().lock();
        try {
            CacheEntry entry = store.get(key);
            return entry != null && !entry.isExpired();
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean expire(String key, long ttlMillis) {
        lock.writeLock().lock();
        try {
            CacheEntry entry = store.get(key);
            if (entry == null || entry.isExpired()) {
                return false;
            }

            store.put(key, entry.withExpiration(ttlMillis));
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long ttl(String key) {
        lock.readLock().lock();
        try {
            CacheEntry entry = store.get(key);
            if (entry == null) {
                return -2; // Key doesn't exist
            }
            return entry.ttl();
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<String> keys(String pattern) {
        lock.readLock().lock();
        try {
            if ("*".equals(pattern)) {
                return new HashSet<>(store.keySet());
            }

            // Simple pattern matching (supports * wildcard)
            String regex = pattern.replace("*", ".*");
            return store.keySet().stream()
                .filter(key -> key.matches(regex))
                .collect(java.util.stream.Collectors.toSet());
        } finally {
            lock.readLock().unlock();
        }
    }

    public long size() {
        return store.size();
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            store.clear();
            accessTimes.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void evictIfNeeded() {
        if (store.size() < maxMemoryBytes / 1024) { // Simple heuristic
            return;
        }

        switch (evictionPolicy) {
            case NO_EVICTION -> {
                // Do nothing, let it grow
            }
            case VOLATILE_LRU -> evictVolatileLRU();
            case ALL_KEYS_LRU -> evictAllKeysLRU();
            case VOLATILE_RANDOM -> evictVolatileRandom();
            case ALL_KEYS_RANDOM -> evictAllKeysRandom();
            case VOLATILE_TTL -> evictVolatileTTL();
        }
    }

    private void evictVolatileLRU() {
        String keyToEvict = store.entrySet().stream()
            .filter(e -> e.getValue().expirationTime() > 0)
            .min(Comparator.comparing(e -> accessTimes.getOrDefault(e.getKey(), 0L)))
            .map(Map.Entry::getKey)
            .orElse(null);

        if (keyToEvict != null) {
            delete(keyToEvict);
            logger.debug("Evicted key: {}", keyToEvict);
        }
    }

    private void evictAllKeysLRU() {
        String keyToEvict = accessTimes.entrySet().stream()
            .min(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null);

        if (keyToEvict != null) {
            delete(keyToEvict);
            logger.debug("Evicted key: {}", keyToEvict);
        }
    }

    private void evictVolatileRandom() {
        List<String> volatileKeys = store.entrySet().stream()
            .filter(e -> e.getValue().expirationTime() > 0)
            .map(Map.Entry::getKey)
            .toList();

        if (!volatileKeys.isEmpty()) {
            String keyToEvict = volatileKeys.get(ThreadLocalRandom.current().nextInt(volatileKeys.size()));
            delete(keyToEvict);
            logger.debug("Evicted key: {}", keyToEvict);
        }
    }

    private void evictAllKeysRandom() {
        List<String> allKeys = new ArrayList<>(store.keySet());
        if (!allKeys.isEmpty()) {
            String keyToEvict = allKeys.get(ThreadLocalRandom.current().nextInt(allKeys.size()));
            delete(keyToEvict);
            logger.debug("Evicted key: {}", keyToEvict);
        }
    }

    private void evictVolatileTTL() {
        String keyToEvict = store.entrySet().stream()
            .filter(e -> e.getValue().expirationTime() > 0)
            .min(Comparator.comparing(e -> e.getValue().ttl()))
            .map(Map.Entry::getKey)
            .orElse(null);

        if (keyToEvict != null) {
            delete(keyToEvict);
            logger.debug("Evicted key: {}", keyToEvict);
        }
    }

    public void shutdown() {
        expirationExecutor.shutdown();
        try {
            if (!expirationExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                expirationExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            expirationExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
