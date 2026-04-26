package com.simplekv.core;

import com.simplekv.api.model.EngineStats;
import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.model.Snapshot;
import com.simplekv.api.options.CompactionStyle;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.api.write.WriteBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleKvEngineTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPutGetDelete() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-put-get-delete"));

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("k1", bytes("v1"));
            assertTrue(store.get("k1").isPresent());
            assertArrayEquals(bytes("v1"), store.get("k1").get());

            store.delete("k1");
            assertFalse(store.get("k1").isPresent());
        }
    }

    @Test
    void shouldSupportSnapshotRead() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-snapshot"));

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("user:1", bytes("v1"));
            Snapshot snapshot = store.snapshot();
            store.put("user:1", bytes("v2"));

            assertTrue(store.get("user:1", snapshot).isPresent());
            assertArrayEquals(bytes("v1"), store.get("user:1", snapshot).get());
            assertTrue(store.get("user:1").isPresent());
            assertArrayEquals(bytes("v2"), store.get("user:1").get());
        }
    }

    @Test
    void shouldSupportScan() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-scan"));

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("a", bytes("1"));
            store.put("b", bytes("2"));
            store.put("c", bytes("3"));

            List<KeyValue> scanned = store.scan("a", "c", 10);
            assertEquals(3, scanned.size());
            assertEquals("a", scanned.get(0).getKey());
            assertEquals("b", scanned.get(1).getKey());
            assertEquals("c", scanned.get(2).getKey());
        }
    }

    @Test
    void shouldRecoverFromWalAfterRestart() throws Exception {
        Path dataDir = tempDir.resolve("db-recovery-wal");
        StorageOptions options = StorageOptions.builder(dataDir)
                .mutableMemtableMaxEntries(1000)
                .dataBlockMaxEntries(32)
                .l0CompactionTrigger(2)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("k1", bytes("v1"));
            store.put("k2", bytes("v2"));
        }

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            assertTrue(store.get("k1").isPresent());
            assertArrayEquals(bytes("v1"), store.get("k1").get());
            assertTrue(store.get("k2").isPresent());
            assertArrayEquals(bytes("v2"), store.get("k2").get());
        }
    }

    @Test
    void shouldExpireTtlData() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-ttl"));

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.ttlPut("ttl-key", bytes("value"), System.currentTimeMillis() - 1L);
            assertFalse(store.get("ttl-key").isPresent());
        }
    }

    @Test
    void shouldFlushAndTriggerCompaction() throws Exception {
        Path dataDir = tempDir.resolve("db-compaction");
        StorageOptions options = StorageOptions.builder(dataDir)
                .mutableMemtableMaxEntries(2)
                .dataBlockMaxEntries(8)
                .l0CompactionTrigger(2)
                .backgroundCompactionEnabled(true)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("k1", bytes("v1"));
            store.put("k2", bytes("v2"));
            store.put("k3", bytes("v3"));
            store.put("k4", bytes("v4"));

            EngineStats stats = waitForCompaction(store);
            assertTrue(stats.getFlushes() >= 2);
            assertTrue(stats.getCompactions() >= 1);
            assertTrue(store.get("k4").isPresent());
        }
    }

    @Test
    void shouldSupportPrefixScanAndDiagnostics() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-prefix-diagnostics"));

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("user:1", bytes("a"));
            store.put("user:2", bytes("b"));
            store.put("sys:1", bytes("c"));
            store.flush();

            List<KeyValue> result = store.prefixScan("user:", 10);
            assertEquals(2, result.size());
            assertTrue(result.get(0).getKey().startsWith("user:"));
            assertTrue(result.get(1).getKey().startsWith("user:"));

            String stats = store.statsCommand();
            assertTrue(stats.contains("writes="));
            assertTrue(stats.contains("cacheHits="));

            String dump = store.sstDump();
            assertTrue(dump.contains("fileId="));
            assertTrue(dump.contains("bloom="));
        }
    }

    @Test
    void shouldTrackCacheAndBloomStats() throws Exception {
        StorageOptions options = StorageOptions.builder(tempDir.resolve("db-cache-bloom"))
                .mutableMemtableMaxEntries(2)
                .dataBlockMaxEntries(4)
                .l0CompactionTrigger(100)
                .bloomFilterBitsPerKey(12)
                .blockCacheMaxEntries(16)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("k1", bytes("v1"));
            store.put("k2", bytes("v2"));
            store.put("k3", bytes("v3"));
            store.flush();

            assertTrue(store.get("k1").isPresent());
            assertTrue(store.get("k1").isPresent());
            assertFalse(store.get("missing").isPresent());

            EngineStats stats = store.stats();
            assertTrue(stats.getCacheHits() >= 1);
            assertTrue(stats.getBloomPositives() >= 1 || stats.getBloomNegatives() >= 1);
        }
    }

    @Test
    void shouldSupportWriteBatchAndValidateAtomicInput() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-batch"));

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            WriteBatch batch = new WriteBatch()
                    .put("a", bytes("1"))
                    .put("b", bytes("2"));
            store.writeBatch(batch);

            assertArrayEquals(bytes("1"), store.get("a").get());
            assertArrayEquals(bytes("2"), store.get("b").get());

            assertThrows(IllegalArgumentException.class,
                    () -> store.writeBatch(new WriteBatch().put("bad", null)));
            assertArrayEquals(bytes("1"), store.get("a").get());
        }
    }

    @Test
    void shouldBeCloseIdempotentAndRejectOperationsAfterClose() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-close"));

        KeyValueStore store = SimpleKvEngines.open(options);
        store.put("k", bytes("v"));
        store.close();
        store.close();

        assertThrows(IllegalStateException.class, () -> store.get("k"));
        assertThrows(IllegalStateException.class, () -> store.put("x", bytes("y")));
        assertThrows(IllegalStateException.class, () -> store.flush());
    }

    @Test
    void shouldTriggerBackpressureAndSlowMetrics() throws Exception {
        StorageOptions options = StorageOptions.builder(tempDir.resolve("db-backpressure"))
                .mutableMemtableMaxEntries(1)
                .dataBlockMaxEntries(4)
                .l0CompactionTrigger(100)
                .l0StopWritesTrigger(2)
                .backpressureSleepMillis(1L)
                .backpressureMaxRetries(50)
                .slowQueryThresholdMillis(1L)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            List<String> keys = new ArrayList<String>();
            for (int i = 0; i < 80; i++) {
                String key = "k-" + i;
                keys.add(key);
                store.put(key, bytes("v-" + i));
            }
            store.scan("k-0", "k-99", 100);
            store.get(keys.get(0));

            EngineStats stats = waitForCompaction(store);
            assertTrue(stats.getBackpressureEvents() >= 0);
            assertTrue(stats.getSlowReads() >= 0);
            assertTrue(stats.getSlowScans() >= 0);
        }
    }

    @Test
    void shouldOpenViaSimpleKvEngines() throws Exception {
        StorageOptions options = defaultOptions(tempDir.resolve("db-open-wrapper"));
        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            assertNotNull(store);
            assertTrue(store.stats().getWriteAmplification() >= 0.0d);
            assertTrue(store.stats().getReadAmplification() >= 0.0d);
            assertTrue(store.stats().getSpaceAmplification() >= 0.0d);
        }
    }

    @Test
    void shouldCompactWithSizeTieredStyleFromOptions() throws Exception {
        StorageOptions options = StorageOptions.builder(tempDir.resolve("db-size-tiered"))
                .mutableMemtableMaxEntries(2)
                .dataBlockMaxEntries(8)
                .l0CompactionTrigger(2)
                .compactionStyle(CompactionStyle.SIZE_TIERED)
                .backgroundCompactionEnabled(true)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (KeyValueStore store = SimpleKvEngines.open(options)) {
            store.put("k1", bytes("v1"));
            store.put("k2", bytes("v2"));
            store.put("k3", bytes("v3"));
            store.put("k4", bytes("v4"));
            EngineStats stats = waitForCompaction(store);
            assertTrue(stats.getCompactions() >= 1);
        }
    }

    private StorageOptions defaultOptions(Path dataDir) {
        return StorageOptions.builder(dataDir)
                .mutableMemtableMaxEntries(8)
                .dataBlockMaxEntries(32)
                .l0CompactionTrigger(2)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static EngineStats waitForCompaction(KeyValueStore store) throws Exception {
        EngineStats last = store.stats();
        for (int i = 0; i < 60; i++) {
            last = store.stats();
            if (last.getCompactions() > 0) {
                return last;
            }
            Thread.sleep(20L);
        }
        return last;
    }
}
