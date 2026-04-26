package com.simplekv.storage.model;

public enum ValueType {
    PUT((byte) 1),
    DELETE((byte) 2);

    private final byte code;

    ValueType(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static ValueType fromCode(byte code) {
        if (code == PUT.code) {
            return PUT;
        }
        if (code == DELETE.code) {
            return DELETE;
        }
        throw new IllegalArgumentException("Unknown value type code: " + code);
    }
}
