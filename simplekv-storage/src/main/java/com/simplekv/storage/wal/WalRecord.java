package com.simplekv.storage.wal;

import com.simplekv.storage.model.ValueType;

import java.util.Arrays;
import java.util.Objects;

public final class WalRecord {
        private final long sequence;
        private final ValueType valueType;
        private final String key;
        private final byte[] value;
        private final long expireAtMillis;

        public WalRecord(long sequence, ValueType valueType, String key, byte[] value, long expireAtMillis) {
                this.sequence = sequence;
                this.valueType = Objects.requireNonNull(valueType, "valueType");
                this.key = Objects.requireNonNull(key, "key");
                this.value = value == null ? null : Arrays.copyOf(value, value.length);
                this.expireAtMillis = expireAtMillis;
        }

        public long sequence() {
                return sequence;
        }

        public ValueType valueType() {
                return valueType;
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
