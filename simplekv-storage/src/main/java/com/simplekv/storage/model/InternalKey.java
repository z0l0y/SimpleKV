package com.simplekv.storage.model;

import java.util.Objects;

public final class InternalKey implements Comparable<InternalKey> {
    private final String userKey;
    private final long sequence;
    private final ValueType valueType;

    public InternalKey(String userKey, long sequence, ValueType valueType) {
        this.userKey = Objects.requireNonNull(userKey, "userKey");
        this.sequence = sequence;
        this.valueType = Objects.requireNonNull(valueType, "valueType");
    }

    public static InternalKey seekKey(String userKey) {
        return new InternalKey(userKey, Long.MAX_VALUE, ValueType.PUT);
    }

    public String userKey() {
        return userKey;
    }

    public long sequence() {
        return sequence;
    }

    public ValueType valueType() {
        return valueType;
    }

    @Override
    public int compareTo(InternalKey other) {
        int keyCompare = this.userKey.compareTo(other.userKey);
        if (keyCompare != 0) {
            return keyCompare;
        }
        int sequenceCompare = Long.compare(other.sequence, this.sequence);
        if (sequenceCompare != 0) {
            return sequenceCompare;
        }
        return Byte.compare(this.valueType.code(), other.valueType.code());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof InternalKey)) {
            return false;
        }
        InternalKey that = (InternalKey) obj;
        return sequence == that.sequence
                && userKey.equals(that.userKey)
                && valueType == that.valueType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userKey, sequence, valueType);
    }

    @Override
    public String toString() {
        return "InternalKey{" +
                "userKey='" + userKey + '\'' +
                ", sequence=" + sequence +
                ", valueType=" + valueType +
                '}';
    }
}
