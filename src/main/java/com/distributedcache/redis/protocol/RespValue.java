package com.distributedcache.redis.protocol;

import java.util.List;

/**
 * Represents a RESP protocol value
 */
public sealed interface RespValue permits
    RespValue.SimpleString,
    RespValue.Error,
    RespValue.Integer,
    RespValue.BulkString,
    RespValue.Array {

    RespType type();

    record SimpleString(String value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.SIMPLE_STRING;
        }
    }

    record Error(String message) implements RespValue {
        @Override
        public RespType type() {
            return RespType.ERROR;
        }
    }

    record Integer(long value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.INTEGER;
        }
    }

    record BulkString(String value) implements RespValue {
        public static final BulkString NULL = new BulkString(null);

        @Override
        public RespType type() {
            return RespType.BULK_STRING;
        }

        public boolean isNull() {
            return value == null;
        }
    }

    record Array(List<RespValue> values) implements RespValue {
        public static final Array NULL = new Array(null);

        @Override
        public RespType type() {
            return RespType.ARRAY;
        }

        public boolean isNull() {
            return values == null;
        }
    }
}
