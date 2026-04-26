package com.simplekv.storage.mem;

import com.simplekv.storage.model.InternalEntry;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemTableTest {

    @Test
    void shouldPutDeleteAndReadLatestVisible() {
        MemTable table = new MemTable();
        assertTrue(table.isEmpty());

        table.put(1L, "k", bytes("v1"), -1L);
        table.put(3L, "k", bytes("v3"), -1L);
        table.delete(4L, "k");

        assertEquals(3, table.size());
        assertFalse(table.isEmpty());

        Optional<InternalEntry> latestAt3 = table.latestEntry("k", 3L);
        assertTrue(latestAt3.isPresent());
        assertArrayEquals(bytes("v3"), latestAt3.get().value().value());

        Optional<InternalEntry> latestAt4 = table.latestEntry("k", 4L);
        assertTrue(latestAt4.isPresent());
        assertEquals(true, latestAt4.get().key().valueType().name().equals("DELETE"));

        assertFalse(table.getVisible("k", 4L, System.currentTimeMillis()).isPresent());
        assertFalse(table.getVisible("missing", 4L, System.currentTimeMillis()).isPresent());
    }

    @Test
    void shouldHandleTtlAndScanVisible() {
        MemTable table = new MemTable();
        long now = System.currentTimeMillis();

        table.put(1L, "a", bytes("1"), -1L);
        table.put(2L, "b", bytes("2"), now - 1L);
        table.put(3L, "c", bytes("3"), -1L);
        table.delete(4L, "d");

        assertFalse(table.getVisible("b", 3L, now).isPresent());

        List<InternalEntry> scannedAll = table.scanVisible("a", "z", 10, 10L, now);
        assertEquals(2, scannedAll.size());
        assertEquals("a", scannedAll.get(0).key().userKey());
        assertEquals("c", scannedAll.get(1).key().userKey());

        List<InternalEntry> scannedLimited = table.scanVisible("a", "z", 1, 10L, now);
        assertEquals(1, scannedLimited.size());

        List<InternalEntry> scannedNone = table.scanVisible("a", "z", 0, 10L, now);
        assertTrue(scannedNone.isEmpty());
    }

    @Test
    void shouldReturnSnapshotEntries() {
        MemTable table = new MemTable();
        table.put(1L, "k1", bytes("v1"), -1L);
        table.put(2L, "k2", bytes("v2"), -1L);

        List<InternalEntry> entries = table.snapshotEntries();
        assertEquals(2, entries.size());
        assertEquals("k1", entries.get(0).key().userKey());
        assertEquals("k2", entries.get(1).key().userKey());
    }

    @Test
    void shouldBreakLatestLookupAndReturnVisibleEntry() {
        MemTable table = new MemTable();
        long now = System.currentTimeMillis();

        table.put(5L, "a", bytes("a5"), -1L);
        table.put(4L, "a", bytes("a4"), -1L);
        table.put(3L, "b", bytes("b3"), -1L);

        assertFalse(table.latestEntry("a", 1L).isPresent());
        assertTrue(table.getVisible("b", 10L, now).isPresent());
    }

    @Test
    void shouldCoverScanBranchingConditions() {
        MemTable table = new MemTable();
        long now = System.currentTimeMillis();

        table.put(10L, "a", bytes("a10"), -1L);
        table.put(7L, "b", bytes("b7"), -1L);
        table.put(6L, "b", bytes("b6"), -1L);
        table.put(5L, "c", bytes("c5"), -1L);

        List<InternalEntry> scanned = table.scanVisible("a", "b", 10, 7L, now);
        assertEquals(1, scanned.size());
        assertEquals("b", scanned.get(0).key().userKey());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
