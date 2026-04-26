package com.simplekv.storage.bloom;

import java.nio.charset.StandardCharsets;
import java.util.BitSet;

public final class SimpleBloomFilter {
    private final int bitSize;
    private final int hashFunctions;
    private final BitSet bits;

    private SimpleBloomFilter(int bitSize, int hashFunctions, BitSet bits) {
        this.bitSize = bitSize;
        this.hashFunctions = hashFunctions;
        this.bits = bits;
    }

    public static SimpleBloomFilter create(int expectedKeys, int bitsPerKey) {
        int normalizedExpected = Math.max(1, expectedKeys);
        int normalizedBitsPerKey = Math.max(1, bitsPerKey);
        int bitSize = Math.max(64, normalizedExpected * normalizedBitsPerKey);
        int hashFunctions = Math.max(1, (int) Math.round(((double) bitSize / normalizedExpected) * Math.log(2.0d)));
        return new SimpleBloomFilter(bitSize, hashFunctions, new BitSet(bitSize));
    }

    public static SimpleBloomFilter restore(int bitSize, int hashFunctions, byte[] bitSetBytes) {
        if (bitSize < 1) {
            throw new IllegalArgumentException("bitSize must be positive");
        }
        if (hashFunctions < 1) {
            throw new IllegalArgumentException("hashFunctions must be positive");
        }
        return new SimpleBloomFilter(bitSize, hashFunctions, BitSet.valueOf(bitSetBytes));
    }

    public void put(String key) {
        int[] indexes = indexes(key);
        for (int index : indexes) {
            bits.set(index);
        }
    }

    public boolean mightContain(String key) {
        int[] indexes = indexes(key);
        for (int index : indexes) {
            if (!bits.get(index)) {
                return false;
            }
        }
        return true;
    }

    public int bitSize() {
        return bitSize;
    }

    public int hashFunctions() {
        return hashFunctions;
    }

    public byte[] bitSetBytes() {
        return bits.toByteArray();
    }

    private int[] indexes(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        int hash1 = fnv1a32(bytes, 0x811c9dc5);
        int hash2 = fnv1a32(bytes, 0x01000193);
        int[] indexes = new int[hashFunctions];
        for (int i = 0; i < hashFunctions; i++) {
            long combined = (hash1 & 0xffffffffL) + (long) i * (hash2 & 0xffffffffL);
            long positive = combined & 0x7fffffffffffffffL;
            indexes[i] = (int) (positive % bitSize);
        }
        return indexes;
    }

    private static int fnv1a32(byte[] bytes, int seed) {
        int hash = seed;
        for (byte b : bytes) {
            hash ^= (b & 0xff);
            hash *= 16777619;
        }
        return hash;
    }
}
