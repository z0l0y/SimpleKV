package com.simplekv.api.model;

import java.util.Arrays;
import java.util.Objects;

public final class KeyValue {
    private final String key;
    private final byte[] value;

    public KeyValue(String key, byte[] value) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "key='" + key + '\'' +
                ", valueLength=" + (value == null ? -1 : value.length) +
                '}';
    }
}
