package com.simplekv.storage.sstable;

import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.InternalKey;
import com.simplekv.storage.model.InternalValue;
import com.simplekv.storage.model.ValueType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SstableRoundTripTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRoundTripEntries() throws Exception {
        SstableWriter writer = new SstableWriter();
        List<InternalEntry> entries = Arrays.asList(
                new InternalEntry(new InternalKey("a", 3L, ValueType.PUT), new InternalValue("v3".getBytes(), -1L)),
                new InternalEntry(new InternalKey("a", 2L, ValueType.DELETE), new InternalValue(null, -1L)),
                new InternalEntry(new InternalKey("b", 1L, ValueType.PUT), new InternalValue("v1".getBytes(), -1L))
        );

        SstableMetadata metadata = writer.write(tempDir, 1L, 0, entries, 2, 10);
        SstableReader reader = SstableReader.open(tempDir.resolve(metadata.getFileName()), metadata);

        assertEquals(3, reader.allEntries().size());
        assertTrue(reader.hasBloomFilter());
        assertTrue(reader.mightContain("a"));
        assertTrue(reader.getVisible("a", 3L, System.currentTimeMillis()).isPresent());
        assertArrayEquals("v3".getBytes(), reader.getVisible("a", 3L, System.currentTimeMillis()).get().value().value());
        assertFalse(reader.getVisible("a", 2L, System.currentTimeMillis()).isPresent());

        List<InternalEntry> scanned = reader.scanVisible("a", "z", 10, 10L, System.currentTimeMillis());
        assertEquals(2, scanned.size());
        assertEquals("a", scanned.get(0).key().userKey());
        assertEquals("b", scanned.get(1).key().userKey());
        assertTrue(reader.scanVisible("a", "z", 0, 10L, System.currentTimeMillis()).isEmpty());
    }

    @Test
    void shouldRejectEmptyEntriesWhenWriting() {
        SstableWriter writer = new SstableWriter();
        assertThrows(IllegalArgumentException.class,
                () -> writer.write(tempDir, 2L, 0, Collections.<InternalEntry>emptyList(), 2, 10));
    }

    @Test
    void shouldThrowForCorruptedSstableFile() throws Exception {
        Path corrupted = tempDir.resolve("broken.sst");
        java.nio.file.Files.write(corrupted, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});

        SstableMetadata metadata = new SstableMetadata(3L, 0, "broken.sst", "a", "z", null, 1L, 2L, 1L);
        assertThrows(Exception.class, () -> SstableReader.open(corrupted, metadata));
    }

    @Test
    void shouldCoverReaderGettersAndBloomFallbacks() throws Exception {
        SstableWriter writer = new SstableWriter();
        List<InternalEntry> entries = Collections.singletonList(
                entry("x", 1L, ValueType.PUT, "vx", -1L)
        );

        SstableMetadata metadata = writer.write(tempDir, 10L, 0, entries, 2, 10);
        Path sstableFile = tempDir.resolve(metadata.getFileName());

        Files.delete(StorageLayout.bloomFileForSstable(sstableFile));

        SstableMetadata noBloomMetadata = copyMetadata(metadata, null);
        SstableReader noBloomReader = SstableReader.open(sstableFile, noBloomMetadata);

        assertFalse(noBloomReader.hasBloomFilter());
        assertTrue(noBloomReader.mightContain("missing-key"));
        assertEquals(sstableFile, noBloomReader.path());
        assertEquals(noBloomMetadata, noBloomReader.metadata());

        Path brokenBloom = tempDir.resolve("broken.bf");
        Files.write(brokenBloom, new byte[]{1, 2, 3});

        SstableMetadata brokenBloomMetadata = copyMetadata(metadata, brokenBloom.getFileName().toString());
        SstableReader brokenBloomReader = SstableReader.open(sstableFile, brokenBloomMetadata);
        assertFalse(brokenBloomReader.hasBloomFilter());
    }

    @Test
    void shouldCoverLatestAndScanBranches() throws Exception {
        long now = System.currentTimeMillis();
        SstableWriter writer = new SstableWriter();
        List<InternalEntry> entries = Arrays.asList(
                entry("a", 5L, ValueType.PUT, "a5", -1L),
                entry("a", 4L, ValueType.PUT, "a4", -1L),
                entry("b", 9L, ValueType.PUT, "b9", -1L),
                entry("c", 3L, ValueType.DELETE, null, -1L),
                entry("d", 2L, ValueType.PUT, "d2", now - 1L),
                entry("e", 1L, ValueType.PUT, "e1", -1L),
                entry("f", 1L, ValueType.PUT, "f1", -1L)
        );

        SstableMetadata metadata = writer.write(tempDir, 11L, 0, entries, 3, 10);
        SstableReader reader = SstableReader.open(tempDir.resolve(metadata.getFileName()), metadata);

        assertFalse(reader.latestEntry("missing", 10L).isPresent());
        assertFalse(reader.latestEntry("a", 3L).isPresent());
        assertFalse(reader.getVisible("missing", 10L, now).isPresent());

        List<InternalEntry> bounded = reader.scanVisible("b", "e", 10, 3L, now);
        assertEquals(1, bounded.size());
        assertEquals("e", bounded.get(0).key().userKey());

        List<InternalEntry> limited = reader.scanVisible("a", "z", 1, 10L, now);
        assertEquals(1, limited.size());
    }

    @Test
    void shouldRejectMalformedSstableShapes() throws Exception {
        Path tooSmall = tempDir.resolve("too-small.sst");
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(tooSmall))) {
            out.writeInt(SstableLayout.MAGIC);
            out.writeInt(SstableLayout.VERSION);
        }
        assertThrows(IOException.class, () -> SstableReader.open(tooSmall, metadataFor(tooSmall, null)));

        Path badFooter = tempDir.resolve("bad-footer.sst");
        writeMalformedSstable(badFooter, new byte[]{0}, 9L, 1, 1, 1L, 0x12345678);
        assertThrows(IOException.class, () -> SstableReader.open(badFooter, metadataFor(badFooter, null)));

        Path badOffsets = tempDir.resolve("bad-offsets.sst");
        writeMalformedSstable(badOffsets, new byte[]{0}, 7L, 1, 1, 1L, SstableLayout.FOOTER_MAGIC);
        assertThrows(IOException.class, () -> SstableReader.open(badOffsets, metadataFor(badOffsets, null)));

        Path badBlockCount = tempDir.resolve("bad-block-count.sst");
        byte[] badBlockCountMiddle = ByteBuffer.allocate(5)
                .putInt(0)
                .put((byte) 0)
                .array();
        writeMalformedSstable(badBlockCount, badBlockCountMiddle, 12L, 1, 1, 1L, SstableLayout.FOOTER_MAGIC);
        assertThrows(IOException.class, () -> SstableReader.open(badBlockCount, metadataFor(badBlockCount, null)));

        Path emptyData = tempDir.resolve("empty-data.sst");
        writeMalformedSstable(emptyData, new byte[]{0}, 8L, 1, 1, 1L, SstableLayout.FOOTER_MAGIC);
        assertThrows(IOException.class, () -> SstableReader.open(emptyData, metadataFor(emptyData, null)));

        Path truncatedEntry = tempDir.resolve("truncated-entry.sst");
        byte[] truncatedMiddle = ByteBuffer.allocate(9)
                .putInt(1)
                .putInt(1000)
                .put((byte) 0)
                .array();
        writeMalformedSstable(truncatedEntry, truncatedMiddle, 16L, 1, 1, 1L, SstableLayout.FOOTER_MAGIC);
        assertThrows(IOException.class, () -> SstableReader.open(truncatedEntry, metadataFor(truncatedEntry, null)));
    }

    private static SstableMetadata copyMetadata(SstableMetadata metadata, String bloomFileName) {
        return new SstableMetadata(
                metadata.getFileId(),
                metadata.getLevel(),
                metadata.getFileName(),
                metadata.getMinKey(),
                metadata.getMaxKey(),
                bloomFileName,
                metadata.getMinSequence(),
                metadata.getMaxSequence(),
                metadata.getEntryCount()
        );
    }

    private static SstableMetadata metadataFor(Path file, String bloomFileName) {
        return new SstableMetadata(
                99L,
                0,
                file.getFileName().toString(),
                "a",
                "z",
                bloomFileName,
                1L,
                1L,
                1L
        );
    }

    private static void writeMalformedSstable(
            Path file,
            byte[] middle,
            long indexOffset,
            int indexLength,
            int blockCount,
            long entryCount,
            int footerMagic
    ) throws IOException {
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(file))) {
            out.writeInt(SstableLayout.MAGIC);
            out.writeInt(SstableLayout.VERSION);
            out.write(middle);
            out.writeLong(indexOffset);
            out.writeInt(indexLength);
            out.writeInt(blockCount);
            out.writeLong(entryCount);
            out.writeInt(footerMagic);
        }
    }

    private static InternalEntry entry(String key, long sequence, ValueType type, String value, long expireAtMillis) {
        byte[] bytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        return new InternalEntry(new InternalKey(key, sequence, type), new InternalValue(bytes, expireAtMillis));
    }
}
