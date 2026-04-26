package com.simplekv.core;

import com.simplekv.api.model.EngineStats;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.model.Snapshot;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.api.write.WriteBatch;
import com.simplekv.api.write.WriteOperation;
import com.simplekv.api.write.WriteOperationType;
import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.cache.LruBlockCache;
import com.simplekv.storage.compaction.LsmCompactor;
import com.simplekv.storage.manifest.ManifestState;
import com.simplekv.storage.manifest.ManifestStore;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.mem.MemTable;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.ValueType;
import com.simplekv.storage.sstable.SstableReader;
import com.simplekv.storage.sstable.SstableWriter;
import com.simplekv.storage.wal.WalReader;
import com.simplekv.storage.wal.WalRecord;
import com.simplekv.storage.wal.WalReplayResult;
import com.simplekv.storage.wal.WalWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public final class SimpleKvEngine implements KeyValueStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleKvEngine.class);

    private final StorageOptions options;
    private final Path dataDir;
    private final ManifestStore manifestStore;
    private final SstableWriter sstableWriter;
    private final LsmCompactor lsmCompactor;
    private final LruBlockCache<String, InternalEntry> blockCache;
    private final ExecutorService compactionExecutor;
    private final ScheduledExecutorService cleanupExecutor;
    private final Object mutex;

    private final AtomicLong sequence;

    private final LongAdder writes;
    private final LongAdder reads;
    private final LongAdder deletes;
    private final LongAdder scans;
    private final LongAdder flushes;
    private final LongAdder compactions;
    private final LongAdder bloomPositives;
    private final LongAdder bloomNegatives;
    private final LongAdder slowReads;
    private final LongAdder slowScans;
    private final LongAdder backpressureEvents;
    private final LongAdder readSourceProbes;
    private final LongAdder readRequests;
    private final LongAdder compactionInputEntries;
    private final LongAdder compactionOutputEntries;

    private ManifestState manifestState;
    private WalWriter walWriter;
    private MemTable mutableMemTable;
    private MemTable immutableMemTable;
    private List<SstableReader> l0Readers;
    private List<SstableReader> l1Readers;

    private volatile boolean compactionRunning;
    private volatile boolean closed;

    private SimpleKvEngine(StorageOptions options) throws IOException {
        this.options = options;
        this.dataDir = options.dataDir();
        this.manifestStore = new ManifestStore(dataDir);
        this.sstableWriter = new SstableWriter();
        this.lsmCompactor = new LsmCompactor(options.compactionStyle());
        this.blockCache = new LruBlockCache<String, InternalEntry>(options.blockCacheMaxEntries());
        this.mutex = new Object();

        if (options.backgroundCompactionEnabled()) {
            this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "simplekv-compaction");
                thread.setDaemon(true);
                return thread;
            });
            this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "simplekv-cleanup");
                thread.setDaemon(true);
                return thread;
            });
        } else {
            this.compactionExecutor = null;
            this.cleanupExecutor = null;
        }

        this.sequence = new AtomicLong();

        this.writes = new LongAdder();
        this.reads = new LongAdder();
        this.deletes = new LongAdder();
        this.scans = new LongAdder();
        this.flushes = new LongAdder();
        this.compactions = new LongAdder();
        this.bloomPositives = new LongAdder();
        this.bloomNegatives = new LongAdder();
        this.slowReads = new LongAdder();
        this.slowScans = new LongAdder();
        this.backpressureEvents = new LongAdder();
        this.readSourceProbes = new LongAdder();
        this.readRequests = new LongAdder();
        this.compactionInputEntries = new LongAdder();
        this.compactionOutputEntries = new LongAdder();

        this.mutableMemTable = new MemTable();
        this.immutableMemTable = null;
        this.l0Readers = new ArrayList<>();
        this.l1Readers = new ArrayList<>();

        Files.createDirectories(dataDir);
        this.manifestState = manifestStore.loadOrCreate();
        loadSstablesFromManifest();

        long maxSequenceInSstable = manifestState.getFiles().stream()
                .mapToLong(SstableMetadata::getMaxSequence)
                .max()
                .orElse(0L);

        WalReplayResult replayResult = replayWal(manifestState.getLastFlushedSequence());
        long maxRecoveredSequence = Math.max(maxSequenceInSstable, replayResult.maxSequence());
        sequence.set(maxRecoveredSequence);
        LOGGER.info("SimpleKV recovered: dataDir={}, compactionStyle={}, maxSequence={}, replayedRecords={}.",
                dataDir, options.compactionStyle(), maxRecoveredSequence, replayResult.appliedRecords());

        this.walWriter = new WalWriter(
                StorageLayout.walFile(dataDir),
                options.fsyncPolicy(),
                options.fsyncEveryMillis()
        );

        if (cleanupExecutor != null) {
            cleanupExecutor.scheduleWithFixedDelay(() -> runPeriodicCleanup(),
                options.periodicCleanupIntervalMillis(),
                options.periodicCleanupIntervalMillis(),
                TimeUnit.MILLISECONDS);
        }
    }

    public static SimpleKvEngine open(StorageOptions options) throws IOException {
        return new SimpleKvEngine(options);
    }

    @Override
    public void put(String key, byte[] value) throws IOException {
        writeBatch(new WriteBatch().put(key, value));
    }

    @Override
    public void ttlPut(String key, byte[] value, long expireAtMillis) throws IOException {
        writeBatch(new WriteBatch().ttlPut(key, value, expireAtMillis));
    }

    @Override
    public Optional<byte[]> get(String key) throws IOException {
        long startedAt = System.nanoTime();
        synchronized (mutex) {
            ensureOpen();
            reads.increment();
            readRequests.increment();

            Optional<InternalEntry> cached = blockCache.get(key);
            if (cached.isPresent()) {
                InternalEntry cachedEntry = cached.get();
                if (cachedEntry.key().valueType() != ValueType.DELETE
                        && !cachedEntry.value().isExpired(System.currentTimeMillis())) {
                    maybeRecordSlowRead(key, startedAt);
                    return Optional.ofNullable(cachedEntry.value().value());
                }
                blockCache.remove(key);
            }

            InternalEntry latest = findLatestVisibleEntry(key, sequence.get(), true);
            Optional<byte[]> value = toVisibleValue(latest, System.currentTimeMillis());
            if (value.isPresent() && latest != null) {
                blockCache.put(key, latest);
            }
            maybeRecordSlowRead(key, startedAt);
            return value;
        }
    }

    @Override
    public Optional<byte[]> get(String key, Snapshot snapshot) throws IOException {
        long startedAt = System.nanoTime();
        synchronized (mutex) {
            ensureOpen();
            reads.increment();
            readRequests.increment();

            InternalEntry latest = findLatestVisibleEntry(key, snapshot.getSequence(), true);
            Optional<byte[]> value = toVisibleValue(latest, System.currentTimeMillis());
            maybeRecordSlowRead(key, startedAt);
            return value;
        }
    }

    @Override
    public void delete(String key) throws IOException {
        writeBatch(new WriteBatch().delete(key));
    }

    @Override
    public List<KeyValue> scan(String startKey, String endKey, int limit) throws IOException {
        return scan(startKey, endKey, limit, snapshot());
    }

    @Override
    public List<KeyValue> scan(String startKey, String endKey, int limit, Snapshot snapshot) throws IOException {
        long startedAt = System.nanoTime();
        synchronized (mutex) {
            ensureOpen();
            scans.increment();
            if (limit <= 0) {
                return Collections.emptyList();
            }

            long visibleSequence = snapshot.getSequence();
            long nowMillis = System.currentTimeMillis();
            Map<String, InternalEntry> latestByKey = new HashMap<>();

            collectLatestVisibleRange(mutableMemTable.snapshotEntries(), startKey, endKey, visibleSequence, latestByKey);
            if (immutableMemTable != null) {
                collectLatestVisibleRange(immutableMemTable.snapshotEntries(), startKey, endKey, visibleSequence, latestByKey);
            }
            for (SstableReader reader : l0Readers) {
                collectLatestVisibleRange(reader.allEntries(), startKey, endKey, visibleSequence, latestByKey);
            }
            for (SstableReader reader : l1Readers) {
                collectLatestVisibleRange(reader.allEntries(), startKey, endKey, visibleSequence, latestByKey);
            }

            List<KeyValue> values = latestByKey.values().stream()
                    .filter(entry -> entry.key().valueType() != ValueType.DELETE)
                    .filter(entry -> !entry.value().isExpired(nowMillis))
                    .sorted(Comparator.comparing(entry -> entry.key().userKey()))
                    .limit(limit)
                    .map(entry -> new KeyValue(entry.key().userKey(), entry.value().value()))
                    .collect(Collectors.toList());

            for (KeyValue kv : values) {
                InternalEntry entry = latestByKey.get(kv.getKey());
                if (entry != null) {
                    blockCache.put(kv.getKey(), entry);
                }
            }

            maybeRecordSlowScan(startKey, endKey, limit, startedAt);
            return values;
        }
    }

    @Override
    public List<KeyValue> prefixScan(String prefix, int limit) throws IOException {
        String normalizedPrefix = prefix == null ? "" : prefix;
        String end = normalizedPrefix + '\uffff';
        List<KeyValue> scanned = scan(normalizedPrefix, end, limit, snapshot());
        return scanned.stream()
                .filter(kv -> kv.getKey().startsWith(normalizedPrefix))
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public void writeBatch(WriteBatch batch) throws IOException {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        synchronized (mutex) {
            ensureOpen();
            applyBackpressureLocked();

            List<WalRecord> walRecords = new ArrayList<>(batch.size());

            for (WriteOperation operation : batch.operations()) {
                validateWriteOperation(operation);
                long sequenceNumber = nextSequence();
                ValueType valueType = operation.type() == WriteOperationType.DELETE ? ValueType.DELETE : ValueType.PUT;
                walRecords.add(new WalRecord(
                        sequenceNumber,
                        valueType,
                        operation.key(),
                        operation.value(),
                        operation.expireAtMillis()
                ));
            }

            walWriter.appendBatch(walRecords);
            for (WalRecord record : walRecords) {
                applyRecord(mutableMemTable, record);
                blockCache.remove(record.key());
                writes.increment();
                if (record.valueType() == ValueType.DELETE) {
                    deletes.increment();
                }
            }

            maybeFlushLocked();
            triggerCompactionLocked(false);
        }
    }

    @Override
    public Snapshot snapshot() {
        return new Snapshot(sequence.get());
    }

    @Override
    public EngineStats stats() {
        synchronized (mutex) {
            long writesCount = writes.sum();
            long readsCount = reads.sum();
            long deletesCount = deletes.sum();
            long scansCount = scans.sum();

            double writeAmp = safeDivide(writesCount + compactionInputEntries.sum(), writesCount);
            double readAmp = safeDivide(readSourceProbes.sum(), readRequests.sum());

            long totalPhysicalEntries = totalPhysicalEntriesLocked();
            long estimatedLogicalEntries = Math.max(1L, writesCount - deletesCount);
            double spaceAmp = safeDivide(totalPhysicalEntries, estimatedLogicalEntries);

            return new EngineStats(
                    writesCount,
                    readsCount,
                    deletesCount,
                    scansCount,
                    flushes.sum(),
                    compactions.sum(),
                    blockCache.hits(),
                    blockCache.misses(),
                    bloomPositives.sum(),
                    bloomNegatives.sum(),
                    slowReads.sum(),
                    slowScans.sum(),
                    backpressureEvents.sum(),
                    mutableMemTable.size(),
                    immutableMemTable == null ? 0 : immutableMemTable.size(),
                    l0Readers.size(),
                    l1Readers.size(),
                    writeAmp,
                    readAmp,
                    spaceAmp
            );
        }
    }

    @Override
    public String statsCommand() {
        EngineStats stats = stats();
        StringBuilder builder = new StringBuilder(512);
        builder.append("SimpleKV Stats\n");
        builder.append("writes=").append(stats.getWrites()).append('\n');
        builder.append("reads=").append(stats.getReads()).append('\n');
        builder.append("deletes=").append(stats.getDeletes()).append('\n');
        builder.append("scans=").append(stats.getScans()).append('\n');
        builder.append("flushes=").append(stats.getFlushes()).append('\n');
        builder.append("compactions=").append(stats.getCompactions()).append('\n');
        builder.append("cacheHits=").append(stats.getCacheHits()).append('\n');
        builder.append("cacheMisses=").append(stats.getCacheMisses()).append('\n');
        builder.append("cacheHitRate=").append(String.format("%.4f", blockCache.hitRate())).append('\n');
        builder.append("bloomPositives=").append(stats.getBloomPositives()).append('\n');
        builder.append("bloomNegatives=").append(stats.getBloomNegatives()).append('\n');
        builder.append("backpressureEvents=").append(stats.getBackpressureEvents()).append('\n');
        builder.append("writeAmplification=").append(String.format("%.4f", stats.getWriteAmplification())).append('\n');
        builder.append("readAmplification=").append(String.format("%.4f", stats.getReadAmplification())).append('\n');
        builder.append("spaceAmplification=").append(String.format("%.4f", stats.getSpaceAmplification())).append('\n');
        builder.append("l0Files=").append(stats.getL0Files()).append('\n');
        builder.append("l1Files=").append(stats.getL1Files()).append('\n');
        return builder.toString();
    }

    @Override
    public String sstDump() {
        synchronized (mutex) {
            List<SstableMetadata> files = new ArrayList<SstableMetadata>(manifestState.getFiles());
            files.sort(Comparator.comparingInt(SstableMetadata::getLevel)
                    .thenComparingLong(SstableMetadata::getFileId));

            StringBuilder builder = new StringBuilder(512);
            builder.append("SST Dump\n");
            for (SstableMetadata file : files) {
                builder.append("fileId=").append(file.getFileId())
                        .append(", level=").append(file.getLevel())
                        .append(", file=").append(file.getFileName())
                        .append(", bloom=").append(file.getBloomFileName())
                        .append(", keyRange=").append(file.getMinKey()).append("..").append(file.getMaxKey())
                        .append(", seqRange=").append(file.getMinSequence()).append("..").append(file.getMaxSequence())
                        .append(", entries=").append(file.getEntryCount())
                        .append('\n');
            }
            return builder.toString();
        }
    }

    @Override
    public void flush() throws IOException {
        synchronized (mutex) {
            ensureOpen();
            flushLocked();
            triggerCompactionLocked(false);
        }
    }

    @Override
    public void close() throws IOException {
        ExecutorService localCompactionExecutor;
        ScheduledExecutorService localCleanupExecutor;
        synchronized (mutex) {
            if (closed) {
                return;
            }
            flushLocked();
            walWriter.sync();
            walWriter.close();
            manifestStore.save(manifestState);
            closed = true;
            mutex.notifyAll();
            localCompactionExecutor = compactionExecutor;
            localCleanupExecutor = cleanupExecutor;
        }

        if (localCleanupExecutor != null) {
            localCleanupExecutor.shutdownNow();
        }
        if (localCompactionExecutor != null) {
            localCompactionExecutor.shutdown();
            try {
                localCompactionExecutor.awaitTermination(5L, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void validateWriteOperation(WriteOperation operation) {
        if (operation.key() == null || operation.key().isEmpty()) {
            throw new IllegalArgumentException("key must not be empty");
        }
        if (operation.type() == WriteOperationType.PUT && operation.value() == null) {
            throw new IllegalArgumentException("value must not be null for put");
        }
    }

    private void applyBackpressureLocked() throws IOException {
        int retries = 0;
        while (l0Readers.size() >= options.l0StopWritesTrigger()) {
            backpressureEvents.increment();
            triggerCompactionLocked(true);
            retries++;
            if (retries > options.backpressureMaxRetries()) {
                throw new IOException("Backpressure triggered: too many L0 files=" + l0Readers.size());
            }

            try {
                mutex.wait(options.backpressureSleepMillis());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for compaction backpressure", ex);
            }
        }
    }

    private long nextSequence() {
        return sequence.incrementAndGet();
    }

    private void maybeFlushLocked() throws IOException {
        if (mutableMemTable.size() >= options.mutableMemtableMaxEntries()) {
            flushLocked();
        }
    }

    private void flushLocked() throws IOException {
        if (mutableMemTable.isEmpty()) {
            return;
        }

        walWriter.sync();

        immutableMemTable = mutableMemTable;
        mutableMemTable = new MemTable();

        List<InternalEntry> flushEntries = immutableMemTable.snapshotEntries();

        long fileId = nextFileIdLocked();
        SstableMetadata metadata = sstableWriter.write(
                dataDir,
                fileId,
                0,
                flushEntries,
            options.dataBlockMaxEntries(),
            options.bloomFilterBitsPerKey()
        );

        Path sstableFile = dataDir.resolve(metadata.getFileName());
        SstableReader reader = SstableReader.open(sstableFile, metadata);

        l0Readers.add(0, reader);
        manifestState.getFiles().add(metadata);

        long maxFlushedSequence = flushEntries.stream().mapToLong(entry -> entry.key().sequence()).max().orElse(0L);
        if (maxFlushedSequence > manifestState.getLastFlushedSequence()) {
            manifestState.setLastFlushedSequence(maxFlushedSequence);
        }

        manifestStore.save(manifestState);

        immutableMemTable = null;
        walWriter.truncate();
        flushes.increment();
        blockCache.clear();
    }

    private void triggerCompactionLocked(boolean force) throws IOException {
        if (!shouldCompactLocked(force)) {
            return;
        }

        if (options.backgroundCompactionEnabled()) {
            scheduleCompactionLocked(force);
            return;
        }

        compactLocked(force);
    }

    private boolean shouldCompactLocked(boolean force) {
        if (l0Readers.size() >= options.l0CompactionTrigger()) {
            return true;
        }
        if (force) {
            return !l0Readers.isEmpty() || !l1Readers.isEmpty();
        }
        return false;
    }

    private void scheduleCompactionLocked(boolean force) {
        if (compactionExecutor == null || compactionRunning) {
            return;
        }

        compactionRunning = true;
        final boolean forceRun = force;
        compactionExecutor.execute(() -> {
            synchronized (mutex) {
                try {
                    if (closed) {
                        return;
                    }
                    compactLocked(forceRun);
                } catch (IOException ex) {
                    LOGGER.warn("Compaction failed", ex);
                } finally {
                    compactionRunning = false;
                    mutex.notifyAll();
                }
            }
        });
    }

    private void compactLocked(boolean force) throws IOException {
        if (!shouldCompactLocked(force)) {
            return;
        }

        List<SstableReader> compactionInputs = new ArrayList<>();
        compactionInputs.addAll(l0Readers);

        
        if (force || !l1Readers.isEmpty()) {
            compactionInputs.addAll(l1Readers);
        }

        LOGGER.info("Compaction started: style={}, force={}, l0Files={}, l1Files={}, inputFiles={}",
                options.compactionStyle(), force, l0Readers.size(), l1Readers.size(), compactionInputs.size());

        long nowMillis = System.currentTimeMillis();
        long inputEntries = 0L;
        for (SstableReader reader : compactionInputs) {
            inputEntries += reader.metadata().getEntryCount();
        }
        compactionInputEntries.add(inputEntries);

        List<InternalEntry> mergedEntries = lsmCompactor.compact(compactionInputs, nowMillis, true);
        compactionOutputEntries.add(mergedEntries.size());

        Set<Long> removedFileIds = compactionInputs.stream().map(reader -> reader.metadata().getFileId()).collect(java.util.stream.Collectors.toSet());
        manifestState.getFiles().removeIf(meta -> removedFileIds.contains(meta.getFileId()));

        for (SstableReader reader : compactionInputs) {
            deleteSstableArtifacts(reader);
        }

        l0Readers = new ArrayList<>();
        l1Readers = new ArrayList<>();

        if (!mergedEntries.isEmpty()) {
            long newFileId = nextFileIdLocked();
            SstableMetadata compactedMetadata = sstableWriter.write(
                    dataDir,
                    newFileId,
                    1,
                    mergedEntries,
                    options.dataBlockMaxEntries(),
                    options.bloomFilterBitsPerKey()
            );
            SstableReader compactedReader = SstableReader.open(dataDir.resolve(compactedMetadata.getFileName()), compactedMetadata);
            l1Readers.add(compactedReader);
            manifestState.getFiles().add(compactedMetadata);
        }

        manifestStore.save(manifestState);

        compactions.increment();
        blockCache.clear();
        LOGGER.info("Compaction finished: style={}, inputEntries={}, outputEntries={}, removedFiles={}, newL0Files={}, newL1Files={}",
                options.compactionStyle(), inputEntries, mergedEntries.size(), removedFileIds.size(), l0Readers.size(), l1Readers.size());
        mutex.notifyAll();
    }

    private long nextFileIdLocked() {
        long fileId = manifestState.getNextFileId();
        manifestState.setNextFileId(fileId + 1);
        return fileId;
    }

    private InternalEntry findLatestVisibleEntry(String key, long visibleSequence, boolean useBloomFilter) {
        InternalEntry latest = null;
        int probes = 0;

        probes++;
        Optional<InternalEntry> mutable = mutableMemTable.latestEntry(key, visibleSequence);
        if (mutable.isPresent()) {
            InternalEntry candidate = mutable.get();
            if (latest == null || candidate.key().sequence() > latest.key().sequence()) {
                latest = candidate;
            }
        }

        if (immutableMemTable != null) {
            probes++;
            Optional<InternalEntry> immutable = immutableMemTable.latestEntry(key, visibleSequence);
            if (immutable.isPresent()) {
                InternalEntry candidate = immutable.get();
                if (latest == null || candidate.key().sequence() > latest.key().sequence()) {
                    latest = candidate;
                }
            }
        }

        for (SstableReader reader : l0Readers) {
            if (useBloomFilter && reader.hasBloomFilter()) {
                if (!reader.mightContain(key)) {
                    bloomNegatives.increment();
                    continue;
                }
                bloomPositives.increment();
            }
            probes++;
            Optional<InternalEntry> entry = reader.latestEntry(key, visibleSequence);
            if (entry.isPresent()) {
                InternalEntry candidate = entry.get();
                if (latest == null || candidate.key().sequence() > latest.key().sequence()) {
                    latest = candidate;
                }
            }
        }

        for (SstableReader reader : l1Readers) {
            if (useBloomFilter && reader.hasBloomFilter()) {
                if (!reader.mightContain(key)) {
                    bloomNegatives.increment();
                    continue;
                }
                bloomPositives.increment();
            }
            probes++;
            Optional<InternalEntry> entry = reader.latestEntry(key, visibleSequence);
            if (entry.isPresent()) {
                InternalEntry candidate = entry.get();
                if (latest == null || candidate.key().sequence() > latest.key().sequence()) {
                    latest = candidate;
                }
            }
        }

        readSourceProbes.add(probes);

        return latest;
    }

    private InternalEntry chooseLatest(InternalEntry current, InternalEntry candidate) {
        if (current == null) {
            return candidate;
        }
        return candidate.key().sequence() > current.key().sequence() ? candidate : current;
    }

    private void collectLatestVisibleRange(
            List<InternalEntry> entries,
            String startKey,
            String endKey,
            long visibleSequence,
            Map<String, InternalEntry> latestByKey
    ) {
        for (InternalEntry entry : entries) {
            String userKey = entry.key().userKey();
            if (userKey.compareTo(startKey) < 0 || userKey.compareTo(endKey) > 0) {
                continue;
            }
            if (entry.key().sequence() > visibleSequence) {
                continue;
            }
            InternalEntry current = latestByKey.get(userKey);
            if (current == null || entry.key().sequence() > current.key().sequence()) {
                latestByKey.put(userKey, entry);
            }
        }
    }

    private void loadSstablesFromManifest() throws IOException {
        l0Readers.clear();
        l1Readers.clear();

        List<SstableMetadata> existing = new ArrayList<>(manifestState.getFiles());
        manifestState.getFiles().clear();

        for (SstableMetadata metadata : existing) {
            Path sstableFile = dataDir.resolve(metadata.getFileName());
            if (!Files.exists(sstableFile)) {
                continue;
            }
            SstableReader reader = SstableReader.open(sstableFile, metadata);
            manifestState.getFiles().add(metadata);
            if (metadata.getLevel() == 0) {
                l0Readers.add(reader);
            } else if (metadata.getLevel() == 1) {
                l1Readers.add(reader);
            }
        }

        l0Readers.sort(Comparator.comparingLong((SstableReader reader) -> reader.metadata().getFileId()).reversed());
        l1Readers.sort(Comparator.comparing(reader -> reader.metadata().getMinKey()));
    }

    private WalReplayResult replayWal(long lastFlushedSequence) throws IOException {
        List<WalRecord> records = WalReader.readAll(StorageLayout.walFile(dataDir));

        long maxSequence = 0L;
        int applied = 0;
        for (WalRecord record : records) {
            maxSequence = Math.max(maxSequence, record.sequence());
            if (record.sequence() <= lastFlushedSequence) {
                continue;
            }
            applyRecord(mutableMemTable, record);
            applied++;
        }
        return new WalReplayResult(maxSequence, applied);
    }

    private void applyRecord(MemTable table, WalRecord record) {
        if (record.valueType() == ValueType.DELETE) {
            table.delete(record.sequence(), record.key());
        } else {
            table.put(record.sequence(), record.key(), record.value(), record.expireAtMillis());
        }
    }

    private void deleteSstableArtifacts(SstableReader reader) throws IOException {
        deletePathIfExists(reader.path());
        Path bloomPath;
        if (reader.metadata().getBloomFileName() != null && !reader.metadata().getBloomFileName().trim().isEmpty()) {
            bloomPath = dataDir.resolve(reader.metadata().getBloomFileName());
        } else {
            bloomPath = StorageLayout.bloomFileForSstable(reader.path());
        }
        deletePathIfExists(bloomPath);
    }

    private void deletePathIfExists(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }

        if (Files.isDirectory(path)) {
            Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (exc != null) {
                        throw exc;
                    }
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
            return;
        }

        Files.deleteIfExists(path);
    }

    private void runPeriodicCleanup() {
        synchronized (mutex) {
            if (closed) {
                return;
            }
            try {
                triggerCompactionLocked(true);
            } catch (IOException ex) {
                LOGGER.warn("Periodic cleanup failed", ex);
            }
        }
    }

    private Optional<byte[]> toVisibleValue(InternalEntry latest, long nowMillis) {
        if (latest == null) {
            return Optional.empty();
        }
        if (latest.key().valueType() == ValueType.DELETE) {
            return Optional.empty();
        }
        if (latest.value().isExpired(nowMillis)) {
            return Optional.empty();
        }
        return Optional.ofNullable(latest.value().value());
    }

    private void maybeRecordSlowRead(String key, long startedAtNanos) {
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
        if (elapsedMillis >= options.slowQueryThresholdMillis()) {
            slowReads.increment();
            LOGGER.warn("Slow get detected: key={}, elapsedMs={}", key, elapsedMillis);
        }
    }

    private void maybeRecordSlowScan(String startKey, String endKey, int limit, long startedAtNanos) {
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
        if (elapsedMillis >= options.slowQueryThresholdMillis()) {
            slowScans.increment();
            LOGGER.warn("Slow scan detected: start={}, end={}, limit={}, elapsedMs={}",
                    startKey, endKey, limit, elapsedMillis);
        }
    }

    private long totalPhysicalEntriesLocked() {
        long total = mutableMemTable.size();
        total += immutableMemTable == null ? 0 : immutableMemTable.size();
        for (SstableMetadata file : manifestState.getFiles()) {
            total += file.getEntryCount();
        }
        return total;
    }

    private static double safeDivide(long numerator, long denominator) {
        if (denominator <= 0L) {
            return 0.0d;
        }
        return (double) numerator / (double) denominator;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("SimpleKvEngine is already closed");
        }
    }
}
