package com.simplekv.app.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.nio.file.Paths;

@ConfigurationProperties(prefix = "simplekv.storage")
public class SimpleKvStorageProperties {
    private Path dataDir = Paths.get("./data");
    private int memTableMaxEntries = 10000;
    private int scanDefaultLimit = 100;
    private int cacheMaxEntries = 4096;
    private boolean bloomFilterEnabled = true;
    private int bloomExpectedInsertions = 100000;
    private double bloomFalsePositiveRate = 0.01d;
    private int level0CompactionTrigger = 4;
    private int level0MaxFiles = 16;
    private int flushBatchSize = 1000;
    private int tombstoneRetentionSeconds = 3600;
    private String compactionStyle = "leveled";

    public Path getDataDir() {
        return dataDir;
    }

    public void setDataDir(Path dataDir) {
        this.dataDir = dataDir;
    }

    public int getMemTableMaxEntries() {
        return memTableMaxEntries;
    }

    public void setMemTableMaxEntries(int memTableMaxEntries) {
        this.memTableMaxEntries = memTableMaxEntries;
    }

    public int getScanDefaultLimit() {
        return scanDefaultLimit;
    }

    public void setScanDefaultLimit(int scanDefaultLimit) {
        this.scanDefaultLimit = scanDefaultLimit;
    }

    public int getCacheMaxEntries() {
        return cacheMaxEntries;
    }

    public void setCacheMaxEntries(int cacheMaxEntries) {
        this.cacheMaxEntries = cacheMaxEntries;
    }

    public boolean isBloomFilterEnabled() {
        return bloomFilterEnabled;
    }

    public void setBloomFilterEnabled(boolean bloomFilterEnabled) {
        this.bloomFilterEnabled = bloomFilterEnabled;
    }

    public int getBloomExpectedInsertions() {
        return bloomExpectedInsertions;
    }

    public void setBloomExpectedInsertions(int bloomExpectedInsertions) {
        this.bloomExpectedInsertions = bloomExpectedInsertions;
    }

    public double getBloomFalsePositiveRate() {
        return bloomFalsePositiveRate;
    }

    public void setBloomFalsePositiveRate(double bloomFalsePositiveRate) {
        this.bloomFalsePositiveRate = bloomFalsePositiveRate;
    }

    public int getLevel0CompactionTrigger() {
        return level0CompactionTrigger;
    }

    public void setLevel0CompactionTrigger(int level0CompactionTrigger) {
        this.level0CompactionTrigger = level0CompactionTrigger;
    }

    public int getLevel0MaxFiles() {
        return level0MaxFiles;
    }

    public void setLevel0MaxFiles(int level0MaxFiles) {
        this.level0MaxFiles = level0MaxFiles;
    }

    public int getFlushBatchSize() {
        return flushBatchSize;
    }

    public void setFlushBatchSize(int flushBatchSize) {
        this.flushBatchSize = flushBatchSize;
    }

    public int getTombstoneRetentionSeconds() {
        return tombstoneRetentionSeconds;
    }

    public void setTombstoneRetentionSeconds(int tombstoneRetentionSeconds) {
        this.tombstoneRetentionSeconds = tombstoneRetentionSeconds;
    }

    public String getCompactionStyle() {
        return compactionStyle;
    }

    public void setCompactionStyle(String compactionStyle) {
        this.compactionStyle = compactionStyle;
    }
}
