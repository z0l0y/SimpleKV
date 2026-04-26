package com.simplekv.api.model;

public final class EngineStats {
        private final long writes;
        private final long reads;
        private final long deletes;
        private final long scans;
        private final long flushes;
        private final long compactions;
        private final long cacheHits;
        private final long cacheMisses;
        private final long bloomPositives;
        private final long bloomNegatives;
        private final long slowReads;
        private final long slowScans;
        private final long backpressureEvents;
        private final int mutableEntries;
        private final int immutableEntries;
        private final int l0Files;
        private final int l1Files;
        private final double writeAmplification;
        private final double readAmplification;
        private final double spaceAmplification;

        public EngineStats(long writes, long reads, long deletes, long scans, long flushes, long compactions,
                           long cacheHits, long cacheMisses, long bloomPositives, long bloomNegatives,
                           long slowReads, long slowScans, long backpressureEvents,
                           int mutableEntries, int immutableEntries, int l0Files, int l1Files,
                           double writeAmplification, double readAmplification, double spaceAmplification) {
                this.writes = writes;
                this.reads = reads;
                this.deletes = deletes;
                this.scans = scans;
                this.flushes = flushes;
                this.compactions = compactions;
                this.cacheHits = cacheHits;
                this.cacheMisses = cacheMisses;
                this.bloomPositives = bloomPositives;
                this.bloomNegatives = bloomNegatives;
                this.slowReads = slowReads;
                this.slowScans = slowScans;
                this.backpressureEvents = backpressureEvents;
                this.mutableEntries = mutableEntries;
                this.immutableEntries = immutableEntries;
                this.l0Files = l0Files;
                this.l1Files = l1Files;
                this.writeAmplification = writeAmplification;
                this.readAmplification = readAmplification;
                this.spaceAmplification = spaceAmplification;
        }

        public long getWrites() {
                return writes;
        }

        public long getReads() {
                return reads;
        }

        public long getDeletes() {
                return deletes;
        }

        public long getScans() {
                return scans;
        }

        public long getFlushes() {
                return flushes;
        }

        public long getCompactions() {
                return compactions;
        }

        public long getCacheHits() {
                return cacheHits;
        }

        public long getCacheMisses() {
                return cacheMisses;
        }

        public long getBloomPositives() {
                return bloomPositives;
        }

        public long getBloomNegatives() {
                return bloomNegatives;
        }

        public long getSlowReads() {
                return slowReads;
        }

        public long getSlowScans() {
                return slowScans;
        }

        public long getBackpressureEvents() {
                return backpressureEvents;
        }

        public int getMutableEntries() {
                return mutableEntries;
        }

        public int getImmutableEntries() {
                return immutableEntries;
        }

        public int getL0Files() {
                return l0Files;
        }

        public int getL1Files() {
                return l1Files;
        }

        public double getWriteAmplification() {
                return writeAmplification;
        }

        public double getReadAmplification() {
                return readAmplification;
        }

        public double getSpaceAmplification() {
                return spaceAmplification;
        }
}
