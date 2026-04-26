package com.simplekv.storage.model;

import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.io.CodecUtils;
import com.simplekv.storage.sstable.SstableLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageModelAndUtilsTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCoverValueTypeBranches() {
        assertEquals(ValueType.PUT, ValueType.fromCode((byte) 1));
        assertEquals(ValueType.DELETE, ValueType.fromCode((byte) 2));
        assertThrows(IllegalArgumentException.class, () -> ValueType.fromCode((byte) 99));
    }

    @Test
    void shouldCompareAndHashInternalKey() {
        InternalKey a1 = new InternalKey("a", 10L, ValueType.PUT);
        InternalKey a2 = new InternalKey("a", 8L, ValueType.PUT);
        InternalKey b1 = new InternalKey("b", 1L, ValueType.PUT);

        assertTrue(a1.compareTo(a2) < 0);
        assertTrue(a2.compareTo(b1) < 0);
        assertEquals(a1, new InternalKey("a", 10L, ValueType.PUT));
        assertNotEquals(a1, b1);
        assertNotEquals(a1, "a");
        assertTrue(a1.hashCode() == new InternalKey("a", 10L, ValueType.PUT).hashCode());
        assertTrue(a1.toString().contains("userKey='a'"));

        assertEquals("a", InternalKey.seekKey("a").userKey());
        assertEquals(Long.MAX_VALUE, InternalKey.seekKey("a").sequence());

        assertThrows(NullPointerException.class, () -> new InternalKey(null, 1L, ValueType.PUT));
        assertThrows(NullPointerException.class, () -> new InternalKey("k", 1L, null));
    }

    @Test
    void shouldHandleInternalValueCopyAndExpire() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        InternalValue internalValue = new InternalValue(value, 100L);
        value[0] = 'X';

        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), internalValue.value());
        byte[] read = internalValue.value();
        read[0] = 'Y';
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), internalValue.value());

        assertEquals(false, internalValue.isExpired(99L));
        assertEquals(true, internalValue.isExpired(100L));

        InternalValue noExpire = new InternalValue(null, -1L);
        assertEquals(null, noExpire.value());
        assertEquals(false, noExpire.isExpired(System.currentTimeMillis()));
    }

    @Test
    void shouldConstructInternalEntry() {
        InternalKey key = new InternalKey("k", 1L, ValueType.PUT);
        InternalValue value = new InternalValue("v".getBytes(StandardCharsets.UTF_8), -1L);
        InternalEntry entry = new InternalEntry(key, value);

        assertEquals(key, entry.key());
        assertEquals(value, entry.value());

        assertThrows(NullPointerException.class, () -> new InternalEntry(null, value));
        assertThrows(NullPointerException.class, () -> new InternalEntry(key, null));
    }

    @Test
    void shouldEncodeAndDecodeCodecUtils() {
        assertArrayEquals("abc".getBytes(StandardCharsets.UTF_8), CodecUtils.stringToBytes("abc"));
        assertEquals("abc", CodecUtils.bytesToString("abc".getBytes(StandardCharsets.UTF_8)));

        ByteBuffer buffer = ByteBuffer.allocate(64);
        CodecUtils.putByteArray(buffer, "v1".getBytes(StandardCharsets.UTF_8));
        CodecUtils.putByteArray(buffer, null);
        buffer.flip();

        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), CodecUtils.getByteArray(buffer));
        assertEquals(null, CodecUtils.getByteArray(buffer));

        assertEquals(Integer.BYTES + 2, CodecUtils.estimateByteArraySize("v1".getBytes(StandardCharsets.UTF_8)));
        assertEquals(Integer.BYTES, CodecUtils.estimateByteArraySize(null));
    }

    @Test
    void shouldResolveStorageLayoutPaths() {
        Path dataDir = tempDir.resolve("db");
        Path sst = StorageLayout.sstableFile(dataDir, 7L, 1);
        assertTrue(StorageLayout.walFile(dataDir).toString().endsWith("wal.log"));
        assertTrue(StorageLayout.currentFile(dataDir).toString().endsWith("CURRENT"));
        assertTrue(StorageLayout.manifestFile(dataDir).toString().endsWith("MANIFEST.json"));
        assertTrue(sst.toString().contains("sst-7-L1.sst"));
        assertTrue(StorageLayout.bloomFileForSstable(sst).toString().endsWith(".sst.bf"));
    }

    @Test
    void shouldCoverSstableLayoutConstants() {
        assertTrue(SstableLayout.MAGIC != 0);
        assertTrue(SstableLayout.VERSION > 0);
        assertTrue(SstableLayout.FOOTER_MAGIC != 0);
        assertTrue(SstableLayout.HEADER_BYTES > 0);
        assertTrue(SstableLayout.FOOTER_BYTES > 0);
    }

    @Test
    void shouldInvokeUtilityPrivateConstructors() throws Exception {
        invokePrivateConstructor(CodecUtils.class);
        invokePrivateConstructor(StorageLayout.class);
        invokePrivateConstructor(SstableLayout.class);
    }

    private static void invokePrivateConstructor(Class<?> type) throws Exception {
        Constructor<?> ctor = type.getDeclaredConstructor();
        ctor.setAccessible(true);
        ctor.newInstance();
    }
}
