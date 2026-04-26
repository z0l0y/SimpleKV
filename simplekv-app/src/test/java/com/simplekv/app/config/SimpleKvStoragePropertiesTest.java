package com.simplekv.app.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SimpleKvStoragePropertiesTest {

    @Test
    void shouldExposeDefaultsAndSetters() {
        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        assertEquals(Paths.get("./data"), properties.getDataDir());
        assertEquals(10000, properties.getMemTableMaxEntries());
        assertEquals(100, properties.getScanDefaultLimit());
        assertEquals(4096, properties.getCacheMaxEntries());
        assertEquals(true, properties.isBloomFilterEnabled());
        assertEquals(100000, properties.getBloomExpectedInsertions());
        assertEquals(0.01d, properties.getBloomFalsePositiveRate());
        assertEquals(4, properties.getLevel0CompactionTrigger());
        assertEquals(16, properties.getLevel0MaxFiles());
        assertEquals(1000, properties.getFlushBatchSize());
        assertEquals(3600, properties.getTombstoneRetentionSeconds());
        assertEquals("leveled", properties.getCompactionStyle());

        properties.setDataDir(Paths.get("./tmp"));
        properties.setMemTableMaxEntries(321);
        properties.setScanDefaultLimit(111);
        properties.setCacheMaxEntries(222);
        properties.setBloomFilterEnabled(false);
        properties.setBloomExpectedInsertions(333);
        properties.setBloomFalsePositiveRate(0.2d);
        properties.setLevel0CompactionTrigger(5);
        properties.setLevel0MaxFiles(12);
        properties.setFlushBatchSize(64);
        properties.setTombstoneRetentionSeconds(30);
        properties.setCompactionStyle("size-tiered");

        assertEquals(Paths.get("./tmp"), properties.getDataDir());
        assertEquals(321, properties.getMemTableMaxEntries());
        assertEquals(111, properties.getScanDefaultLimit());
        assertEquals(222, properties.getCacheMaxEntries());
        assertFalse(properties.isBloomFilterEnabled());
        assertEquals(333, properties.getBloomExpectedInsertions());
        assertEquals(0.2d, properties.getBloomFalsePositiveRate());
        assertEquals(5, properties.getLevel0CompactionTrigger());
        assertEquals(12, properties.getLevel0MaxFiles());
        assertEquals(64, properties.getFlushBatchSize());
        assertEquals(30, properties.getTombstoneRetentionSeconds());
        assertEquals("size-tiered", properties.getCompactionStyle());
    }
}
