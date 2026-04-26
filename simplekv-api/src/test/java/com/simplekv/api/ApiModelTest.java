package com.simplekv.api;

import com.simplekv.api.model.EngineStats;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.model.Snapshot;
import com.simplekv.api.options.CompactionStyle;
import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.api.write.WriteBatch;
import com.simplekv.api.write.WriteOperation;
import com.simplekv.api.write.WriteOperationType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiModelTest {

    @Test
    void shouldDefensivelyCopyKeyValueBytes() {
        byte[] origin = "value".getBytes(StandardCharsets.UTF_8);
        KeyValue keyValue = new KeyValue("k", origin);
        assertEquals("k", keyValue.getKey());

        origin[0] = 'X';
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), keyValue.getValue());

        byte[] read = keyValue.getValue();
        read[0] = 'Y';
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), keyValue.getValue());
        assertTrue(keyValue.toString().contains("key='k'"));
    }

    @Test
    void shouldCreateWriteOperations() {
        byte[] value = "v1".getBytes(StandardCharsets.UTF_8);
        WriteOperation put = WriteOperation.put("k1", value);
        WriteOperation ttlPut = WriteOperation.ttlPut("k2", value, 100L);
        WriteOperation delete = WriteOperation.delete("k3");

        value[0] = 'X';

        assertEquals(WriteOperationType.PUT, put.type());
        assertEquals("k1", put.key());
        assertArrayEquals("v1".getBytes(StandardCharsets.UTF_8), put.value());
        assertEquals(-1L, put.expireAtMillis());

        assertEquals(WriteOperationType.PUT, ttlPut.type());
        assertEquals(100L, ttlPut.expireAtMillis());

        assertEquals(WriteOperationType.DELETE, delete.type());
        assertEquals("k3", delete.key());
        assertEquals(null, delete.value());
    }

    @Test
    void shouldBuildWriteBatch() {
        WriteBatch batch = new WriteBatch()
                .put("k1", "v1".getBytes(StandardCharsets.UTF_8))
                .ttlPut("k2", "v2".getBytes(StandardCharsets.UTF_8), 123L)
                .delete("k3");

        assertEquals(3, batch.size());
        assertEquals(false, batch.isEmpty());
        assertEquals(WriteOperationType.PUT, batch.operations().get(0).type());
        assertEquals(WriteOperationType.PUT, batch.operations().get(1).type());
        assertEquals(WriteOperationType.DELETE, batch.operations().get(2).type());
    }

    @Test
    void shouldExposeSnapshotAndStats() {
        Snapshot snapshot = new Snapshot(77L);
        assertEquals(77L, snapshot.getSequence());

        EngineStats stats = new EngineStats(
                1L, 2L, 3L, 4L, 5L, 6L,
                7L, 8L, 9L, 10L,
                11L, 12L, 13L,
                14, 15, 16, 17,
                1.1d, 2.2d, 3.3d
        );
        assertEquals(1L, stats.getWrites());
        assertEquals(2L, stats.getReads());
        assertEquals(3L, stats.getDeletes());
        assertEquals(4L, stats.getScans());
        assertEquals(5L, stats.getFlushes());
        assertEquals(6L, stats.getCompactions());
        assertEquals(7L, stats.getCacheHits());
        assertEquals(8L, stats.getCacheMisses());
        assertEquals(9L, stats.getBloomPositives());
        assertEquals(10L, stats.getBloomNegatives());
        assertEquals(11L, stats.getSlowReads());
        assertEquals(12L, stats.getSlowScans());
        assertEquals(13L, stats.getBackpressureEvents());
        assertEquals(14, stats.getMutableEntries());
        assertEquals(15, stats.getImmutableEntries());
        assertEquals(16, stats.getL0Files());
        assertEquals(17, stats.getL1Files());
        assertEquals(1.1d, stats.getWriteAmplification());
        assertEquals(2.2d, stats.getReadAmplification());
        assertEquals(3.3d, stats.getSpaceAmplification());
    }

    @Test
    void shouldBuildStorageOptionsWithDefaultsAndOverrides() {
        StorageOptions defaults = StorageOptions.builder(Paths.get("data")).build();
        assertNotNull(defaults.dataDir());
        assertEquals(4096, defaults.mutableMemtableMaxEntries());
        assertEquals(128, defaults.dataBlockMaxEntries());
        assertEquals(4, defaults.l0CompactionTrigger());
        assertEquals(16, defaults.l0StopWritesTrigger());
        assertEquals(10, defaults.bloomFilterBitsPerKey());
        assertEquals(1024, defaults.blockCacheMaxEntries());
        assertEquals(10L, defaults.backpressureSleepMillis());
        assertEquals(100, defaults.backpressureMaxRetries());
        assertEquals(true, defaults.backgroundCompactionEnabled());
        assertEquals(3000L, defaults.periodicCleanupIntervalMillis());
        assertEquals(20L, defaults.slowQueryThresholdMillis());
        assertEquals(FsyncPolicy.EVERY_N_MILLIS, defaults.fsyncPolicy());
        assertEquals(50L, defaults.fsyncEveryMillis());
        assertEquals(CompactionStyle.LEVELED, defaults.compactionStyle());

        StorageOptions custom = StorageOptions.builder(Paths.get("db"))
                .mutableMemtableMaxEntries(10)
                .dataBlockMaxEntries(11)
                .l0CompactionTrigger(3)
                .l0StopWritesTrigger(6)
                .bloomFilterBitsPerKey(12)
                .blockCacheMaxEntries(33)
                .backpressureSleepMillis(4L)
                .backpressureMaxRetries(5)
                .backgroundCompactionEnabled(false)
                .periodicCleanupIntervalMillis(6L)
                .slowQueryThresholdMillis(7L)
                .fsyncPolicy(FsyncPolicy.MANUAL)
                .fsyncEveryMillis(8L)
                .compactionStyle(CompactionStyle.LEVELED)
                .build();

        assertEquals(10, custom.mutableMemtableMaxEntries());
        assertEquals(11, custom.dataBlockMaxEntries());
        assertEquals(3, custom.l0CompactionTrigger());
        assertEquals(6, custom.l0StopWritesTrigger());
        assertEquals(12, custom.bloomFilterBitsPerKey());
        assertEquals(33, custom.blockCacheMaxEntries());
        assertEquals(4L, custom.backpressureSleepMillis());
        assertEquals(5, custom.backpressureMaxRetries());
        assertEquals(false, custom.backgroundCompactionEnabled());
        assertEquals(6L, custom.periodicCleanupIntervalMillis());
        assertEquals(7L, custom.slowQueryThresholdMillis());
        assertEquals(FsyncPolicy.MANUAL, custom.fsyncPolicy());
        assertEquals(8L, custom.fsyncEveryMillis());
    }

    @Test
    void shouldValidateStorageOptions() {
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).mutableMemtableMaxEntries(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).dataBlockMaxEntries(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).l0CompactionTrigger(1));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).l0StopWritesTrigger(1));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).bloomFilterBitsPerKey(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).blockCacheMaxEntries(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).backpressureSleepMillis(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).backpressureMaxRetries(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).periodicCleanupIntervalMillis(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).slowQueryThresholdMillis(0));
        assertThrows(IllegalArgumentException.class, () -> StorageOptions.builder(Paths.get("db")).fsyncEveryMillis(0));
    }

    @Test
    void shouldCoverEnums() {
        assertTrue(FsyncPolicy.valueOf("ALWAYS") == FsyncPolicy.ALWAYS);
        assertTrue(CompactionStyle.valueOf("LEVELED") == CompactionStyle.LEVELED);
        assertTrue(CompactionStyle.valueOf("SIZE_TIERED") == CompactionStyle.SIZE_TIERED);
        assertEquals(CompactionStyle.LEVELED, CompactionStyle.parse("leveled"));
        assertEquals(CompactionStyle.SIZE_TIERED, CompactionStyle.parse("size-tiered"));
        assertEquals(CompactionStyle.SIZE_TIERED, CompactionStyle.parse("size-tired"));
        assertTrue(WriteOperationType.valueOf("PUT") == WriteOperationType.PUT);
        assertTrue(WriteOperationType.valueOf("DELETE") == WriteOperationType.DELETE);
    }
}
