package com.simplekv.storage.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class CodecUtils {
    private CodecUtils() {
    }

    public static byte[] stringToBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void putByteArray(ByteBuffer buffer, byte[] value) {
        if (value == null) {
            buffer.putInt(-1);
            return;
        }
        buffer.putInt(value.length);
        buffer.put(value);
    }

    public static byte[] getByteArray(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) {
            return new byte[0];
        }
        byte[] value = new byte[length];
        buffer.get(value);
        return value;
    }

    public static int estimateByteArraySize(byte[] value) {
        return Integer.BYTES + (value == null ? 0 : value.length);
    }
}
