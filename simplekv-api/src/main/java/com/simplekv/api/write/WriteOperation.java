package com.simplekv.api.write;

import java.util.Arrays;
import java.util.Objects;

public final class WriteOperation {
    private final WriteOperationType type;
    private final String key;
    private final byte[] value;
    private final long expireAtMillis;

    private WriteOperation(WriteOperationType type, String key, byte[] value, long expireAtMillis) {
        this.type = Objects.requireNonNull(type, "type");
        this.key = Objects.requireNonNull(key, "key");
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
        this.expireAtMillis = expireAtMillis;
    }

    public static WriteOperation put(String key, byte[] value) {
        return new WriteOperation(WriteOperationType.PUT, key, value, -1L);
    }

    public static WriteOperation ttlPut(String key, byte[] value, long expireAtMillis) {
        return new WriteOperation(WriteOperationType.PUT, key, value, expireAtMillis);
    }

    public static WriteOperation delete(String key) {
        return new WriteOperation(WriteOperationType.DELETE, key, null, -1L);
    }

    public WriteOperationType type() {
        return type;
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    public long expireAtMillis() {
        return expireAtMillis;
    }
}
