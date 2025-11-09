package com.distributedcache.redis;

import com.distributedcache.redis.core.DataStore;
import com.distributedcache.redis.core.EvictionPolicy;
import com.distributedcache.redis.core.RedisValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class DataStoreTest {

    private DataStore dataStore;

    @BeforeEach
    void setUp() {
        dataStore = new DataStore(EvictionPolicy.VOLATILE_LRU, 1024 * 1024);
    }

    @AfterEach
    void tearDown() {
        dataStore.shutdown();
    }

    @Test
    void testSetAndGet() {
        String key = "test-key";
        String value = "test-value";

        dataStore.set(key, new RedisValue.StringValue(value));

        Optional<RedisValue> result = dataStore.get(key);
        assertTrue(result.isPresent());
        assertTrue(result.get() instanceof RedisValue.StringValue);
        assertEquals(value, ((RedisValue.StringValue) result.get()).value());
    }

    @Test
    void testDelete() {
        String key = "test-key";
        dataStore.set(key, new RedisValue.StringValue("value"));

        assertTrue(dataStore.exists(key));
        assertTrue(dataStore.delete(key));
        assertFalse(dataStore.exists(key));
    }

    @Test
    void testExpiration() throws InterruptedException {
        String key = "test-key";
        dataStore.set(key, new RedisValue.StringValue("value"), 100); // 100ms TTL

        assertTrue(dataStore.exists(key));

        Thread.sleep(150);

        assertFalse(dataStore.exists(key));
        assertTrue(dataStore.get(key).isEmpty());
    }

    @Test
    void testTTL() {
        String key = "test-key";
        dataStore.set(key, new RedisValue.StringValue("value"), 5000);

        long ttl = dataStore.ttl(key);
        assertTrue(ttl > 0 && ttl <= 5000);
    }

    @Test
    void testKeys() {
        dataStore.set("user:1", new RedisValue.StringValue("alice"));
        dataStore.set("user:2", new RedisValue.StringValue("bob"));
        dataStore.set("product:1", new RedisValue.StringValue("laptop"));

        assertEquals(3, dataStore.keys("*").size());
        assertEquals(2, dataStore.keys("user:.*").size());
        assertEquals(1, dataStore.keys("product:.*").size());
    }
}
