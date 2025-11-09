package com.distributedcache.redis.protocol;

/**
 * RESP (REdis Serialization Protocol) data types
 */
public enum RespType {
    SIMPLE_STRING('+'),
    ERROR('-'),
    INTEGER(':'),
    BULK_STRING('$'),
    ARRAY('*');

    private final char prefix;

    RespType(char prefix) {
        this.prefix = prefix;
    }

    public char prefix() {
        return prefix;
    }

    public static RespType fromPrefix(char prefix) {
        return switch (prefix) {
            case '+' -> SIMPLE_STRING;
            case '-' -> ERROR;
            case ':' -> INTEGER;
            case '$' -> BULK_STRING;
            case '*' -> ARRAY;
            default -> throw new IllegalArgumentException("Unknown RESP prefix: " + prefix);
        };
    }
}
