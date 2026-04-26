package com.simplekv.storage.compaction;

import com.simplekv.api.options.CompactionStyle;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.InternalKey;
import com.simplekv.storage.model.InternalValue;
import com.simplekv.storage.model.ValueType;
import com.simplekv.storage.sstable.SstableReader;
import com.simplekv.storage.sstable.SstableWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LsmCompactorTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCompactWithAndWithoutCleanup() throws Exception {
        long now = System.currentTimeMillis();

        SstableReader r1 = createReader(1L, Arrays.asList(
                entry("a", 5L, ValueType.PUT, "v5", -1L),
                entry("b", 4L, ValueType.PUT, "v4", -1L),
                entry("c", 3L, ValueType.DELETE, null, -1L)
        ));

        SstableReader r2 = createReader(2L, Arrays.asList(
                entry("a", 2L, ValueType.PUT, "v2", -1L),
                entry("b", 1L, ValueType.PUT, "v1", now - 1L),
            entry("d", 6L, ValueType.PUT, "v6", -1L),
            entry("e", 7L, ValueType.PUT, "v7", now - 1L)
        ));

        LsmCompactor compactor = new LsmCompactor();

        List<InternalEntry> keepAll = compactor.compact(Arrays.asList(r1, r2), now, false);
        assertEquals(5, keepAll.size());

        List<InternalEntry> cleanup = compactor.compact(Arrays.asList(r1, r2), now, true);
        assertEquals(3, cleanup.size());
        assertEquals("a", cleanup.get(0).key().userKey());
        assertEquals("b", cleanup.get(1).key().userKey());
        assertEquals("d", cleanup.get(2).key().userKey());

        List<InternalEntry> empty = compactor.compact(Collections.<SstableReader>emptyList());
        assertTrue(empty.isEmpty());
    }

    @Test
    void shouldCompactWithLeveledStyle() throws Exception {
        long now = System.currentTimeMillis();

        SstableReader r1 = createReader(1L, Arrays.asList(
                entry("a", 3L, ValueType.PUT, "v3", -1L),
                entry("b", 2L, ValueType.PUT, "v2", -1L)
        ));

        SstableReader r2 = createReader(2L, Arrays.asList(
                entry("a", 1L, ValueType.PUT, "v1", -1L),
                entry("c", 4L, ValueType.PUT, "v4", -1L)
        ));

        LsmCompactor compactor = new LsmCompactor(CompactionStyle.LEVELED);
        List<InternalEntry> result = compactor.compact(Arrays.asList(r1, r2), now, false);

        assertEquals(3, result.size());
        assertEquals("a", result.get(0).key().userKey());
        assertEquals(3L, result.get(0).key().sequence());
        assertEquals("b", result.get(1).key().userKey());
        assertEquals("c", result.get(2).key().userKey());
    }

    @Test
    void shouldCompactWithSizeTieredStyle() throws Exception {
        long now = System.currentTimeMillis();

        SstableReader r1 = createReader(1L, Arrays.asList(
                entry("a", 3L, ValueType.PUT, "v3", -1L),
                entry("b", 2L, ValueType.PUT, "v2", -1L)
        ));

        SstableReader r2 = createReader(2L, Arrays.asList(
                entry("a", 1L, ValueType.PUT, "v1", -1L),
                entry("c", 4L, ValueType.PUT, "v4", -1L)
        ));

        LsmCompactor compactor = new LsmCompactor(CompactionStyle.SIZE_TIERED);
        List<InternalEntry> result = compactor.compact(Arrays.asList(r1, r2), now, false);

        assertEquals(3, result.size());
        assertEquals("a", result.get(0).key().userKey());
        assertEquals(3L, result.get(0).key().sequence());
        assertEquals("b", result.get(1).key().userKey());
        assertEquals("c", result.get(2).key().userKey());
    }

    @Test
    void shouldHandleEmptyInputForBothStyles() {
        LsmCompactor leveled = new LsmCompactor(CompactionStyle.LEVELED);
        LsmCompactor sizeTiered = new LsmCompactor(CompactionStyle.SIZE_TIERED);

        assertTrue(leveled.compact(Collections.emptyList()).isEmpty());
        assertTrue(sizeTiered.compact(Collections.emptyList()).isEmpty());
    }

    @Test
    void shouldDropDeletedAndExpiredInBothStyles() throws Exception {
        long now = System.currentTimeMillis();

        SstableReader r1 = createReader(1L, Arrays.asList(
                entry("a", 3L, ValueType.DELETE, null, -1L),
                entry("b", 2L, ValueType.PUT, "v2", now - 1000L)
        ));

        SstableReader r2 = createReader(2L, Arrays.asList(
                entry("c", 4L, ValueType.PUT, "v4", -1L)
        ));

        LsmCompactor leveled = new LsmCompactor(CompactionStyle.LEVELED);
        LsmCompactor sizeTiered = new LsmCompactor(CompactionStyle.SIZE_TIERED);

        List<InternalEntry> leveledResult = leveled.compact(Arrays.asList(r1, r2), now, true);
        List<InternalEntry> sizeTieredResult = sizeTiered.compact(Arrays.asList(r1, r2), now, true);

        assertEquals(1, leveledResult.size());
        assertEquals("c", leveledResult.get(0).key().userKey());

        assertEquals(1, sizeTieredResult.size());
        assertEquals("c", sizeTieredResult.get(0).key().userKey());
    }

    private SstableReader createReader(long fileId, List<InternalEntry> entries) throws Exception {
        SstableWriter writer = new SstableWriter();
        SstableMetadata metadata = writer.write(tempDir, fileId, 0, entries, 4, 10);
        return SstableReader.open(tempDir.resolve(metadata.getFileName()), metadata);
    }

    private static InternalEntry entry(String key, long seq, ValueType type, String value, long expireAt) {
        byte[] bytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        return new InternalEntry(new InternalKey(key, seq, type), new InternalValue(bytes, expireAt));
    }
}
