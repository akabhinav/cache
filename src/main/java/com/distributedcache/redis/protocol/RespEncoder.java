package com.distributedcache.redis.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Encoder for RESP (REdis Serialization Protocol)
 */
public class RespEncoder {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    public static byte[] encode(RespValue value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        encode(value, baos);
        return baos.toByteArray();
    }

    public static void encode(RespValue value, OutputStream out) throws IOException {
        switch (value) {
            case RespValue.SimpleString s -> encodeSimpleString(s, out);
            case RespValue.Error e -> encodeError(e, out);
            case RespValue.Integer i -> encodeInteger(i, out);
            case RespValue.BulkString b -> encodeBulkString(b, out);
            case RespValue.Array a -> encodeArray(a, out);
        }
    }

    private static void encodeSimpleString(RespValue.SimpleString value, OutputStream out) throws IOException {
        out.write(RespType.SIMPLE_STRING.prefix());
        out.write(value.value().getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    private static void encodeError(RespValue.Error value, OutputStream out) throws IOException {
        out.write(RespType.ERROR.prefix());
        out.write(value.message().getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    private static void encodeInteger(RespValue.Integer value, OutputStream out) throws IOException {
        out.write(RespType.INTEGER.prefix());
        out.write(String.valueOf(value.value()).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    private static void encodeBulkString(RespValue.BulkString value, OutputStream out) throws IOException {
        out.write(RespType.BULK_STRING.prefix());

        if (value.isNull()) {
            out.write("-1".getBytes(StandardCharsets.UTF_8));
            out.write(CRLF);
            return;
        }

        byte[] data = value.value().getBytes(StandardCharsets.UTF_8);
        out.write(String.valueOf(data.length).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
        out.write(data);
        out.write(CRLF);
    }

    private static void encodeArray(RespValue.Array value, OutputStream out) throws IOException {
        out.write(RespType.ARRAY.prefix());

        if (value.isNull()) {
            out.write("-1".getBytes(StandardCharsets.UTF_8));
            out.write(CRLF);
            return;
        }

        out.write(String.valueOf(value.values().size()).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);

        for (RespValue element : value.values()) {
            encode(element, out);
        }
    }

    // Utility methods for common responses
    public static byte[] ok() throws IOException {
        return encode(new RespValue.SimpleString("OK"));
    }

    public static byte[] error(String message) throws IOException {
        return encode(new RespValue.Error(message));
    }

    public static byte[] integer(long value) throws IOException {
        return encode(new RespValue.Integer(value));
    }

    public static byte[] bulkString(String value) throws IOException {
        return encode(new RespValue.BulkString(value));
    }

    public static byte[] nullBulkString() throws IOException {
        return encode(RespValue.BulkString.NULL);
    }

    public static byte[] array(List<String> values) throws IOException {
        List<RespValue> elements = values.stream()
            .map(RespValue.BulkString::new)
            .map(RespValue.class::cast)
            .toList();
        return encode(new RespValue.Array(elements));
    }
}
