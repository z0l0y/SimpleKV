package com.simplekv.app.config;

import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.options.CompactionStyle;
import com.simplekv.api.options.StorageOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleKvConfigurationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldBuildStorageOptionsFromProperties() {
        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        properties.setDataDir(Paths.get("./data-config-test"));
        properties.setMemTableMaxEntries(2048);
        properties.setFlushBatchSize(128);
        properties.setLevel0CompactionTrigger(6);
        properties.setLevel0MaxFiles(20);
        properties.setBloomFilterEnabled(false);
        properties.setBloomFalsePositiveRate(5.0d);
        properties.setCacheMaxEntries(8192);
        properties.setTombstoneRetentionSeconds(10);
        properties.setCompactionStyle("size-tired");

        SimpleKvConfiguration configuration = new SimpleKvConfiguration();
        StorageOptions options = configuration.storageOptions(properties);

        assertEquals(Paths.get("./data-config-test"), options.dataDir());
        assertEquals(2048, options.mutableMemtableMaxEntries());
        assertEquals(128, options.dataBlockMaxEntries());
        assertEquals(6, options.l0CompactionTrigger());
        assertEquals(20, options.l0StopWritesTrigger());
        assertEquals(1, options.bloomFilterBitsPerKey());
        assertEquals(8192, options.blockCacheMaxEntries());
        assertEquals(10000L, options.periodicCleanupIntervalMillis());
        assertEquals(CompactionStyle.SIZE_TIERED, options.compactionStyle());
    }

    @Test
    void shouldOpenRawStoreWithEnabledBloomConfig() throws Exception {
        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        properties.setDataDir(tempDir);
        properties.setBloomFilterEnabled(true);
        properties.setBloomFalsePositiveRate(0.1d);

        SimpleKvConfiguration configuration = new SimpleKvConfiguration();
        StorageOptions options = configuration.storageOptions(properties);
        assertTrue(options.bloomFilterBitsPerKey() > 1);

        KeyValueStore store = configuration.keyValueStore(options);
        store.close();
    }

    @Test
    void shouldFallbackBloomRateWhenInvalid() {
        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        properties.setBloomFilterEnabled(true);
        properties.setBloomFalsePositiveRate(5.0d);

        SimpleKvConfiguration configuration = new SimpleKvConfiguration();
        StorageOptions options = configuration.storageOptions(properties);

        assertEquals(10, options.bloomFilterBitsPerKey());
    }

    @Test
    void shouldDefaultToLeveledCompactionStyle() {
        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        SimpleKvConfiguration configuration = new SimpleKvConfiguration();
        StorageOptions options = configuration.storageOptions(properties);

        assertEquals(CompactionStyle.LEVELED, options.compactionStyle());
    }
}
