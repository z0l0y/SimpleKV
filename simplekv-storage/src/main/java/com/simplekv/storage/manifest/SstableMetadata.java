package com.simplekv.storage.manifest;

import java.util.Objects;

public class SstableMetadata {
    private long fileId;
    private int level;
    private String fileName;
    private String minKey;
    private String maxKey;
    private String bloomFileName;
    private long minSequence;
    private long maxSequence;
    private long entryCount;

    public SstableMetadata() {
    }

    public SstableMetadata(long fileId, int level, String fileName, String minKey, String maxKey, String bloomFileName,
                           long minSequence, long maxSequence, long entryCount) {
        this.fileId = fileId;
        this.level = level;
        this.fileName = fileName;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.bloomFileName = bloomFileName;
        this.minSequence = minSequence;
        this.maxSequence = maxSequence;
        this.entryCount = entryCount;
    }

    public long getFileId() {
        return fileId;
    }

    public void setFileId(long fileId) {
        this.fileId = fileId;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getMinKey() {
        return minKey;
    }

    public void setMinKey(String minKey) {
        this.minKey = minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    public void setMaxKey(String maxKey) {
        this.maxKey = maxKey;
    }

    public String getBloomFileName() {
        return bloomFileName;
    }

    public void setBloomFileName(String bloomFileName) {
        this.bloomFileName = bloomFileName;
    }

    public long getMinSequence() {
        return minSequence;
    }

    public void setMinSequence(long minSequence) {
        this.minSequence = minSequence;
    }

    public long getMaxSequence() {
        return maxSequence;
    }

    public void setMaxSequence(long maxSequence) {
        this.maxSequence = maxSequence;
    }

    public long getEntryCount() {
        return entryCount;
    }

    public void setEntryCount(long entryCount) {
        this.entryCount = entryCount;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SstableMetadata)) {
            return false;
        }
        SstableMetadata that = (SstableMetadata) other;
        return fileId == that.fileId
                && level == that.level
                && minSequence == that.minSequence
                && maxSequence == that.maxSequence
                && entryCount == that.entryCount
                && Objects.equals(fileName, that.fileName)
                && Objects.equals(minKey, that.minKey)
                && Objects.equals(maxKey, that.maxKey)
                && Objects.equals(bloomFileName, that.bloomFileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileId, level, fileName, minKey, maxKey, bloomFileName, minSequence, maxSequence, entryCount);
    }
}
