package com.simplekv.api.write;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class WriteBatch {
    private final List<WriteOperation> operations = new ArrayList<>();

    public WriteBatch put(String key, byte[] value) {
        operations.add(WriteOperation.put(key, value));
        return this;
    }

    public WriteBatch ttlPut(String key, byte[] value, long expireAtMillis) {
        operations.add(WriteOperation.ttlPut(key, value, expireAtMillis));
        return this;
    }

    public WriteBatch delete(String key) {
        operations.add(WriteOperation.delete(key));
        return this;
    }

    public boolean isEmpty() {
        return operations.isEmpty();
    }

    public int size() {
        return operations.size();
    }

    public List<WriteOperation> operations() {
        return Collections.unmodifiableList(operations);
    }
}
