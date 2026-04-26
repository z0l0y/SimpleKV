package com.simplekv.api.options;

import java.nio.file.Path;
import java.util.Objects;

public final class StorageOptions {
    private final Path dataDir;
    private final int mutableMemtableMaxEntries;
    private final int dataBlockMaxEntries;
    private final int l0CompactionTrigger;
    private final int l0StopWritesTrigger;
    private final int bloomFilterBitsPerKey;
    private final int blockCacheMaxEntries;
    private final long backpressureSleepMillis;
    private final int backpressureMaxRetries;
    private final boolean backgroundCompactionEnabled;
    private final long periodicCleanupIntervalMillis;
    private final long slowQueryThresholdMillis;
    private final FsyncPolicy fsyncPolicy;
    private final long fsyncEveryMillis;
    private final CompactionStyle compactionStyle;

    private StorageOptions(Builder builder) {
        this.dataDir = Objects.requireNonNull(builder.dataDir, "dataDir");
        this.mutableMemtableMaxEntries = builder.mutableMemtableMaxEntries;
        this.dataBlockMaxEntries = builder.dataBlockMaxEntries;
        this.l0CompactionTrigger = builder.l0CompactionTrigger;
        this.l0StopWritesTrigger = builder.l0StopWritesTrigger;
        this.bloomFilterBitsPerKey = builder.bloomFilterBitsPerKey;
        this.blockCacheMaxEntries = builder.blockCacheMaxEntries;
        this.backpressureSleepMillis = builder.backpressureSleepMillis;
        this.backpressureMaxRetries = builder.backpressureMaxRetries;
        this.backgroundCompactionEnabled = builder.backgroundCompactionEnabled;
        this.periodicCleanupIntervalMillis = builder.periodicCleanupIntervalMillis;
        this.slowQueryThresholdMillis = builder.slowQueryThresholdMillis;
        this.fsyncPolicy = builder.fsyncPolicy;
        this.fsyncEveryMillis = builder.fsyncEveryMillis;
        this.compactionStyle = builder.compactionStyle;
    }

    public static Builder builder(Path dataDir) {
        return new Builder(dataDir);
    }

    public Path dataDir() {
        return dataDir;
    }

    public int mutableMemtableMaxEntries() {
        return mutableMemtableMaxEntries;
    }

    public int dataBlockMaxEntries() {
        return dataBlockMaxEntries;
    }

    public int l0CompactionTrigger() {
        return l0CompactionTrigger;
    }

    public int l0StopWritesTrigger() {
        return l0StopWritesTrigger;
    }

    public int bloomFilterBitsPerKey() {
        return bloomFilterBitsPerKey;
    }

    public int blockCacheMaxEntries() {
        return blockCacheMaxEntries;
    }

    public long backpressureSleepMillis() {
        return backpressureSleepMillis;
    }

    public int backpressureMaxRetries() {
        return backpressureMaxRetries;
    }

    public boolean backgroundCompactionEnabled() {
        return backgroundCompactionEnabled;
    }

    public long periodicCleanupIntervalMillis() {
        return periodicCleanupIntervalMillis;
    }

    public long slowQueryThresholdMillis() {
        return slowQueryThresholdMillis;
    }

    public FsyncPolicy fsyncPolicy() {
        return fsyncPolicy;
    }

    public long fsyncEveryMillis() {
        return fsyncEveryMillis;
    }

    public CompactionStyle compactionStyle() {
        return compactionStyle;
    }

    public static final class Builder {
        private final Path dataDir;
        private int mutableMemtableMaxEntries = 4096;
        private int dataBlockMaxEntries = 128;
        private int l0CompactionTrigger = 4;
        private int l0StopWritesTrigger = 16;
        private int bloomFilterBitsPerKey = 10;
        private int blockCacheMaxEntries = 1024;
        private long backpressureSleepMillis = 10L;
        private int backpressureMaxRetries = 100;
        private boolean backgroundCompactionEnabled = true;
        private long periodicCleanupIntervalMillis = 3000L;
        private long slowQueryThresholdMillis = 20L;
        private FsyncPolicy fsyncPolicy = FsyncPolicy.EVERY_N_MILLIS;
        private long fsyncEveryMillis = 50L;
        private CompactionStyle compactionStyle = CompactionStyle.LEVELED;

        private Builder(Path dataDir) {
            this.dataDir = dataDir;
        }

        public Builder mutableMemtableMaxEntries(int mutableMemtableMaxEntries) {
            if (mutableMemtableMaxEntries < 1) {
                throw new IllegalArgumentException("mutableMemtableMaxEntries must be positive");
            }
            this.mutableMemtableMaxEntries = mutableMemtableMaxEntries;
            return this;
        }

        public Builder dataBlockMaxEntries(int dataBlockMaxEntries) {
            if (dataBlockMaxEntries < 1) {
                throw new IllegalArgumentException("dataBlockMaxEntries must be positive");
            }
            this.dataBlockMaxEntries = dataBlockMaxEntries;
            return this;
        }

        public Builder l0CompactionTrigger(int l0CompactionTrigger) {
            if (l0CompactionTrigger < 2) {
                throw new IllegalArgumentException("l0CompactionTrigger must be at least 2");
            }
            this.l0CompactionTrigger = l0CompactionTrigger;
            return this;
        }

        public Builder l0StopWritesTrigger(int l0StopWritesTrigger) {
            if (l0StopWritesTrigger < 2) {
                throw new IllegalArgumentException("l0StopWritesTrigger must be at least 2");
            }
            this.l0StopWritesTrigger = l0StopWritesTrigger;
            return this;
        }

        public Builder bloomFilterBitsPerKey(int bloomFilterBitsPerKey) {
            if (bloomFilterBitsPerKey < 1) {
                throw new IllegalArgumentException("bloomFilterBitsPerKey must be positive");
            }
            this.bloomFilterBitsPerKey = bloomFilterBitsPerKey;
            return this;
        }

        public Builder blockCacheMaxEntries(int blockCacheMaxEntries) {
            if (blockCacheMaxEntries < 1) {
                throw new IllegalArgumentException("blockCacheMaxEntries must be positive");
            }
            this.blockCacheMaxEntries = blockCacheMaxEntries;
            return this;
        }

        public Builder backpressureSleepMillis(long backpressureSleepMillis) {
            if (backpressureSleepMillis < 1) {
                throw new IllegalArgumentException("backpressureSleepMillis must be positive");
            }
            this.backpressureSleepMillis = backpressureSleepMillis;
            return this;
        }

        public Builder backpressureMaxRetries(int backpressureMaxRetries) {
            if (backpressureMaxRetries < 1) {
                throw new IllegalArgumentException("backpressureMaxRetries must be positive");
            }
            this.backpressureMaxRetries = backpressureMaxRetries;
            return this;
        }

        public Builder backgroundCompactionEnabled(boolean backgroundCompactionEnabled) {
            this.backgroundCompactionEnabled = backgroundCompactionEnabled;
            return this;
        }

        public Builder periodicCleanupIntervalMillis(long periodicCleanupIntervalMillis) {
            if (periodicCleanupIntervalMillis < 1) {
                throw new IllegalArgumentException("periodicCleanupIntervalMillis must be positive");
            }
            this.periodicCleanupIntervalMillis = periodicCleanupIntervalMillis;
            return this;
        }

        public Builder slowQueryThresholdMillis(long slowQueryThresholdMillis) {
            if (slowQueryThresholdMillis < 1) {
                throw new IllegalArgumentException("slowQueryThresholdMillis must be positive");
            }
            this.slowQueryThresholdMillis = slowQueryThresholdMillis;
            return this;
        }

        public Builder fsyncPolicy(FsyncPolicy fsyncPolicy) {
            this.fsyncPolicy = Objects.requireNonNull(fsyncPolicy, "fsyncPolicy");
            return this;
        }

        public Builder fsyncEveryMillis(long fsyncEveryMillis) {
            if (fsyncEveryMillis < 1) {
                throw new IllegalArgumentException("fsyncEveryMillis must be positive");
            }
            this.fsyncEveryMillis = fsyncEveryMillis;
            return this;
        }

        public Builder compactionStyle(CompactionStyle compactionStyle) {
            this.compactionStyle = Objects.requireNonNull(compactionStyle, "compactionStyle");
            return this;
        }

        public StorageOptions build() {
            return new StorageOptions(this);
        }
    }
}
