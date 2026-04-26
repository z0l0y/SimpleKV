package com.simplekv.storage.wal;

import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.storage.model.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalWriterReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldWriteAndReadWalRecords() throws Exception {
        Path walFile = tempDir.resolve("wal.log");

        try (WalWriter writer = new WalWriter(walFile, FsyncPolicy.ALWAYS, 1L)) {
            writer.append(new WalRecord(1L, ValueType.PUT, "k1", "v1".getBytes(), -1L));
            writer.append(new WalRecord(2L, ValueType.DELETE, "k1", null, -1L));
        }

        List<WalRecord> records = WalReader.readAll(walFile);
        assertEquals(2, records.size());
        assertEquals(1L, records.get(0).sequence());
        assertEquals(ValueType.PUT, records.get(0).valueType());
        assertEquals("k1", records.get(0).key());
        assertArrayEquals("v1".getBytes(), records.get(0).value());

        assertEquals(2L, records.get(1).sequence());
        assertEquals(ValueType.DELETE, records.get(1).valueType());
        assertEquals("k1", records.get(1).key());
    }

    @Test
    void shouldSupportBatchAppendAndTruncate() throws Exception {
        Path walFile = tempDir.resolve("batch.log");

        try (WalWriter writer = new WalWriter(walFile, FsyncPolicy.MANUAL, 100L)) {
            writer.appendBatch(Arrays.asList(
                    new WalRecord(1L, ValueType.PUT, "a", "1".getBytes(), -1L),
                    new WalRecord(2L, ValueType.PUT, "b", "2".getBytes(), -1L)
            ));
            writer.appendBatch(Collections.<WalRecord>emptyList());
            writer.sync();
        }

        List<WalRecord> records = WalReader.readAll(walFile);
        assertEquals(2, records.size());
        assertEquals("a", records.get(0).key());
        assertEquals("b", records.get(1).key());

        try (WalWriter writer = new WalWriter(walFile, FsyncPolicy.EVERY_N_MILLIS, 1L)) {
            writer.truncate();
            writer.append(new WalRecord(3L, ValueType.PUT, "c", "3".getBytes(), -1L));
        }

        List<WalRecord> afterTruncate = WalReader.readAll(walFile);
        assertEquals(1, afterTruncate.size());
        assertEquals(3L, afterTruncate.get(0).sequence());
    }

    @Test
    void shouldCoverEveryNMillisForcedSyncBranch() throws Exception {
        Path walFile = tempDir.resolve("force-sync.log");

        try (WalWriter writer = new WalWriter(walFile, FsyncPolicy.EVERY_N_MILLIS, Long.MAX_VALUE)) {
            writer.append(new WalRecord(1L, ValueType.PUT, "k", "v".getBytes(), -1L));
            writer.sync();
        }

        assertEquals(1, WalReader.readAll(walFile).size());
    }

    @Test
    void shouldHandleMissingAndCorruptedWal() throws Exception {
        Path missing = tempDir.resolve("missing.log");
        assertTrue(WalReader.readAll(missing).isEmpty());

        Path corrupted = tempDir.resolve("corrupted.log");
        Files.write(corrupted, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
        List<WalRecord> records = WalReader.readAll(corrupted);
        assertTrue(records.isEmpty());
    }

    @Test
    void shouldHandleMalformedWalFrames() throws Exception {
        Path partialHeader = tempDir.resolve("partial-header.log");
        Files.write(partialHeader, ByteBuffer.allocate(Integer.BYTES).putInt(123).array());
        assertTrue(WalReader.readAll(partialHeader).isEmpty());

        Path invalidLength = tempDir.resolve("invalid-length.log");
        writeFrame(invalidLength, 0, 0, new byte[0]);
        assertTrue(WalReader.readAll(invalidLength).isEmpty());

        Path tooLarge = tempDir.resolve("too-large.log");
        writeFrame(tooLarge, (32 * 1024 * 1024) + 1, 0, new byte[0]);
        assertTrue(WalReader.readAll(tooLarge).isEmpty());

        Path badChecksum = tempDir.resolve("bad-checksum.log");
        byte[] zeroCountPayload = ByteBuffer.allocate(Integer.BYTES).putInt(0).array();
        writeFrame(badChecksum, zeroCountPayload.length, 123456789, zeroCountPayload);
        assertTrue(WalReader.readAll(badChecksum).isEmpty());

        Path zeroBatch = tempDir.resolve("zero-batch.log");
        writeFrame(zeroBatch, zeroCountPayload.length, checksum(zeroCountPayload), zeroCountPayload);
        assertTrue(WalReader.readAll(zeroBatch).isEmpty());
    }

    @Test
    void shouldExposeWalReplayResultFields() {
        WalReplayResult result = new WalReplayResult(9L, 3);
        assertEquals(9L, result.maxSequence());
        assertEquals(3, result.appliedRecords());
    }

    @Test
    void shouldInvokeWalReaderPrivateConstructor() throws Exception {
        Constructor<WalReader> constructor = WalReader.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object reader = constructor.newInstance();
        assertFalse(reader == null);
    }

    private static void writeFrame(Path file, int payloadLength, int checksum, byte[] payload) throws Exception {
        ByteBuffer frame = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + payload.length);
        frame.putInt(payloadLength);
        frame.putInt(checksum);
        frame.put(payload);
        Files.write(file, frame.array());
    }

    private static int checksum(byte[] payload) {
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        return (int) crc32.getValue();
    }
}
