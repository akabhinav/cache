package com.distributedcache.redis.core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Represents different types of values that can be stored in Redis
 */
public sealed interface RedisValue permits
    RedisValue.StringValue,
    RedisValue.ListValue,
    RedisValue.SetValue,
    RedisValue.SortedSetValue,
    RedisValue.HashValue {

    ValueType type();

    enum ValueType {
        STRING, LIST, SET, SORTED_SET, HASH
    }

    record StringValue(String value) implements RedisValue {
        @Override
        public ValueType type() {
            return ValueType.STRING;
        }
    }

    record ListValue(List<String> values) implements RedisValue {
        public ListValue() {
            this(new CopyOnWriteArrayList<>());
        }

        @Override
        public ValueType type() {
            return ValueType.LIST;
        }
    }

    record SetValue(Set<String> values) implements RedisValue {
        public SetValue() {
            this(ConcurrentHashMap.newKeySet());
        }

        @Override
        public ValueType type() {
            return ValueType.SET;
        }
    }

    record SortedSetValue(Map<String, Double> values) implements RedisValue {
        public SortedSetValue() {
            this(new ConcurrentHashMap<>());
        }

        public List<Map.Entry<String, Double>> getSorted() {
            return values.entrySet().stream()
                .sorted(Map.Entry.comparingByValue())
                .toList();
        }

        @Override
        public ValueType type() {
            return ValueType.SORTED_SET;
        }
    }

    record HashValue(Map<String, String> fields) implements RedisValue {
        public HashValue() {
            this(new ConcurrentHashMap<>());
        }

        @Override
        public ValueType type() {
            return ValueType.HASH;
        }
    }
}
