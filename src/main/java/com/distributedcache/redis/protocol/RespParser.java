package com.distributedcache.redis.protocol;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for RESP (REdis Serialization Protocol)
 */
public class RespParser {

    private final BufferedReader reader;

    public RespParser(InputStream inputStream) {
        this.reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
    }

    public RespValue parse() throws IOException {
        int firstByte = reader.read();
        if (firstByte == -1) {
            throw new IOException("End of stream");
        }

        char prefix = (char) firstByte;
        RespType type = RespType.fromPrefix(prefix);

        return switch (type) {
            case SIMPLE_STRING -> parseSimpleString();
            case ERROR -> parseError();
            case INTEGER -> parseInteger();
            case BULK_STRING -> parseBulkString();
            case ARRAY -> parseArray();
        };
    }

    private RespValue.SimpleString parseSimpleString() throws IOException {
        String line = reader.readLine();
        return new RespValue.SimpleString(line);
    }

    private RespValue.Error parseError() throws IOException {
        String line = reader.readLine();
        return new RespValue.Error(line);
    }

    private RespValue.Integer parseInteger() throws IOException {
        String line = reader.readLine();
        return new RespValue.Integer(Long.parseLong(line));
    }

    private RespValue.BulkString parseBulkString() throws IOException {
        String lengthStr = reader.readLine();
        int length = Integer.parseInt(lengthStr);

        if (length == -1) {
            return RespValue.BulkString.NULL;
        }

        char[] buffer = new char[length];
        int totalRead = 0;
        while (totalRead < length) {
            int read = reader.read(buffer, totalRead, length - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            totalRead += read;
        }

        // Read trailing \r\n
        reader.readLine();

        return new RespValue.BulkString(new String(buffer));
    }

    private RespValue.Array parseArray() throws IOException {
        String lengthStr = reader.readLine();
        int length = Integer.parseInt(lengthStr);

        if (length == -1) {
            return RespValue.Array.NULL;
        }

        List<RespValue> elements = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            elements.add(parse());
        }

        return new RespValue.Array(elements);
    }
}
