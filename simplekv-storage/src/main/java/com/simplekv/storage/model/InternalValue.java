package com.simplekv.storage.model;

import java.util.Arrays;

public final class InternalValue {
    private final byte[] value;
    private final long expireAtMillis;

    public InternalValue(byte[] value, long expireAtMillis) {
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
        this.expireAtMillis = expireAtMillis;
    }

    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    public long expireAtMillis() {
        return expireAtMillis;
    }

    public boolean isExpired(long nowMillis) {
        return expireAtMillis > 0 && nowMillis >= expireAtMillis;
    }
}
