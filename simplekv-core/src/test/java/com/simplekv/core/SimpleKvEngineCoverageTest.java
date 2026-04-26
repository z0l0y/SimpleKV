package com.simplekv.core;

import com.simplekv.api.model.EngineStats;
import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.api.write.WriteBatch;
import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.bloom.SimpleBloomFilter;
import com.simplekv.storage.cache.LruBlockCache;
import com.simplekv.storage.manifest.ManifestState;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.mem.MemTable;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.InternalKey;
import com.simplekv.storage.model.InternalValue;
import com.simplekv.storage.model.ValueType;
import com.simplekv.storage.sstable.SstableReader;
import com.simplekv.storage.sstable.SstableWriter;
import com.simplekv.storage.wal.WalRecord;
import com.simplekv.storage.wal.WalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleKvEngineCoverageTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCoverGuardsAndInlineCompactionWhenBackgroundDisabled() throws Exception {
        StorageOptions options = StorageOptions.builder(tempDir.resolve("guards-inline"))
                .mutableMemtableMaxEntries(1)
                .dataBlockMaxEntries(4)
                .l0CompactionTrigger(2)
                .backgroundCompactionEnabled(false)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (SimpleKvEngine engine = SimpleKvEngine.open(options)) {
            engine.writeBatch(null);
            engine.writeBatch(new WriteBatch());

            assertTrue(engine.scan("a", "z", 0).isEmpty());
            assertThrows(IllegalArgumentException.class,
                    () -> engine.writeBatch(new WriteBatch().put("", bytes("x"))));

            engine.put("a", bytes("1"));
            engine.put("b", bytes("2"));
            engine.put("c", bytes("3"));

            assertTrue(engine.stats().getCompactions() >= 1);
        }
    }

    @Test
    void shouldCoverCacheImmutableSelectionAndBloomBranches() throws Exception {
        Path dataDir = tempDir.resolve("cache-immutable-bloom");
        StorageOptions options = StorageOptions.builder(dataDir)
                .mutableMemtableMaxEntries(8)
                .dataBlockMaxEntries(4)
                .l0CompactionTrigger(4)
                .backgroundCompactionEnabled(false)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (SimpleKvEngine engine = SimpleKvEngine.open(options)) {
            @SuppressWarnings("unchecked")
            LruBlockCache<String, InternalEntry> cache =
                    (LruBlockCache<String, InternalEntry>) getField(engine, "blockCache");
            cache.put("stale", entry("stale", 1L, ValueType.DELETE, null, -1L));
            assertFalse(engine.get("stale").isPresent());

            MemTable immutable = new MemTable();
            immutable.put(2L, "imm-key", bytes("imm"), -1L);
            setField(engine, "immutableMemTable", immutable);
            ((AtomicLong) getField(engine, "sequence")).set(10L);
            assertTrue(engine.get("imm-key").isPresent());
            assertFalse(engine.scan("a", "z", 10).isEmpty());

            InternalEntry latest = entry("k", 10L, ValueType.PUT, "v10", -1L);
            InternalEntry older = entry("k", 9L, ValueType.PUT, "v9", -1L);
            InternalEntry chosen = (InternalEntry) invokePrivate(
                    engine,
                    "chooseLatest",
                    new Class<?>[]{InternalEntry.class, InternalEntry.class},
                    latest,
                    older
            );
            assertEquals(10L, chosen.key().sequence());

            List<InternalEntry> rangeEntries = Arrays.asList(
                    entry("a", 1L, ValueType.PUT, "a", -1L),
                    entry("b", 99L, ValueType.PUT, "b", -1L),
                    entry("z", 1L, ValueType.PUT, "z", -1L)
            );
            Map<String, InternalEntry> latestByKey = new HashMap<String, InternalEntry>();
            invokePrivate(
                    engine,
                    "collectLatestVisibleRange",
                    new Class<?>[]{List.class, String.class, String.class, long.class, Map.class},
                    rangeEntries,
                    "b",
                    "c",
                    1L,
                    latestByKey
            );
            assertTrue(latestByKey.isEmpty());

            SstableWriter writer = new SstableWriter();
            List<InternalEntry> sstableEntries = Collections.singletonList(
                    entry("present", 1L, ValueType.PUT, "v", -1L)
            );
            SstableMetadata metadata = writer.write(dataDir, 50L, 1, sstableEntries, 4, 10);
            SstableReader reader = SstableReader.open(dataDir.resolve(metadata.getFileName()), metadata);

            setField(engine, "l0Readers", new ArrayList<SstableReader>());
            setField(engine, "l1Readers", new ArrayList<SstableReader>(Collections.singletonList(reader)));

            boolean negativeSeen = false;
            for (int i = 0; i < 300 && !negativeSeen; i++) {
                engine.get("missing-" + i);
                negativeSeen = engine.stats().getBloomNegatives() > 0;
            }
            assertTrue(negativeSeen);
        }
    }

    @Test
    void shouldCoverBackpressureFailureAndInterruptedWait() throws Exception {
        StorageOptions options = backpressureOptions(tempDir.resolve("backpressure-failure"));
        try (SimpleKvEngine engine = SimpleKvEngine.open(options)) {
            engine.put("k1", bytes("1"));
            engine.put("k2", bytes("2"));
            setField(engine, "compactionRunning", Boolean.TRUE);

            assertThrows(IOException.class, () -> engine.put("k3", bytes("3")));
            setField(engine, "compactionRunning", Boolean.FALSE);
        }

        StorageOptions interruptedOptions = backpressureOptions(tempDir.resolve("backpressure-interrupted"));
        SimpleKvEngine interruptedEngine = SimpleKvEngine.open(interruptedOptions);
        try {
            interruptedEngine.put("k1", bytes("1"));
            interruptedEngine.put("k2", bytes("2"));
            setField(interruptedEngine, "compactionRunning", Boolean.TRUE);

            Thread.currentThread().interrupt();
            assertThrows(IOException.class, () -> interruptedEngine.put("k3", bytes("3")));
        } finally {
            Thread.interrupted();
            setField(interruptedEngine, "compactionRunning", Boolean.FALSE);
            interruptedEngine.close();
        }
    }

    @Test
    void shouldCoverInterruptedCloseAwaitTerminationBranch() throws Exception {
        StorageOptions options = StorageOptions.builder(tempDir.resolve("close-interrupted"))
                .backgroundCompactionEnabled(true)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        SimpleKvEngine engine = SimpleKvEngine.open(options);
        try {
            @SuppressWarnings("unchecked")
            ExecutorService compactionExecutor = (ExecutorService) getField(engine, "compactionExecutor");
            compactionExecutor.submit(() -> {
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            });

            Thread owner = Thread.currentThread();
            Thread interrupter = new Thread(() -> {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                owner.interrupt();
            });
            interrupter.start();
            engine.close();
            interrupter.join();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void shouldCoverReplayLoadManifestAndDeleteFallbackBranches() throws Exception {
        Path replayDir = tempDir.resolve("replay-wal");
        StorageOptions replayOptions = StorageOptions.builder(replayDir)
                .mutableMemtableMaxEntries(1)
                .dataBlockMaxEntries(4)
                .l0CompactionTrigger(4)
                .backgroundCompactionEnabled(false)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (SimpleKvEngine engine = SimpleKvEngine.open(replayOptions)) {
            engine.put("base", bytes("v1"));
            engine.flush();
        }

        Path walFile = StorageLayout.walFile(replayDir);
        try (WalWriter writer = new WalWriter(walFile, FsyncPolicy.ALWAYS, 1L)) {
            writer.append(new WalRecord(1L, ValueType.PUT, "skip", bytes("s"), -1L));
            writer.append(new WalRecord(2L, ValueType.PUT, "apply", bytes("a"), -1L));
        }

        try (SimpleKvEngine reopened = SimpleKvEngine.open(replayOptions)) {
            assertFalse(reopened.get("skip").isPresent());
            assertTrue(reopened.get("apply").isPresent());
        }

        Path loadDir = tempDir.resolve("load-manifest");
        StorageOptions loadOptions = StorageOptions.builder(loadDir)
                .backgroundCompactionEnabled(false)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (SimpleKvEngine engine = SimpleKvEngine.open(loadOptions)) {
            SstableWriter writer = new SstableWriter();
            List<InternalEntry> entries = Collections.singletonList(
                    entry("l1-key", 1L, ValueType.PUT, "v", -1L)
            );
            SstableMetadata level1 = writer.write(loadDir, 200L, 1, entries, 4, 10);
            SstableMetadata missing = new SstableMetadata(201L, 0, "missing.sst", "a", "z", null, 1L, 1L, 1L);

            ManifestState state = (ManifestState) getField(engine, "manifestState");
            state.getFiles().clear();
            state.getFiles().add(missing);
            state.getFiles().add(level1);

            invokePrivate(engine, "loadSstablesFromManifest", new Class<?>[0]);

            @SuppressWarnings("unchecked")
            List<SstableReader> l0 = (List<SstableReader>) getField(engine, "l0Readers");
            @SuppressWarnings("unchecked")
            List<SstableReader> l1 = (List<SstableReader>) getField(engine, "l1Readers");

            assertEquals(0, l0.size());
            assertEquals(1, l1.size());
        }

        Path artifactRoot = tempDir.resolve("delete-fallback");
        Files.createDirectories(artifactRoot);
        Path fakeSstable = artifactRoot.resolve("fallback.sst");
        Files.write(fakeSstable, bytes("sst"));
        Path fallbackBloom = StorageLayout.bloomFileForSstable(fakeSstable);
        Files.write(fallbackBloom, bytes("bf"));

        SstableMetadata fakeMeta = new SstableMetadata(
                300L,
                0,
                fakeSstable.getFileName().toString(),
                "a",
                "a",
                null,
                1L,
                1L,
                1L
        );
        SstableReader fakeReader = newFakeReader(
                fakeSstable,
                fakeMeta,
                Collections.singletonList(entry("a", 1L, ValueType.PUT, "v", -1L))
        );

        StorageOptions deleteOptions = StorageOptions.builder(artifactRoot.resolve("db-delete"))
                .backgroundCompactionEnabled(false)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (SimpleKvEngine engine = SimpleKvEngine.open(deleteOptions)) {
            invokePrivate(engine, "deleteSstableArtifacts", new Class<?>[]{SstableReader.class}, fakeReader);
        }

        assertFalse(Files.exists(fakeSstable));
        assertFalse(Files.exists(fallbackBloom));
    }

    @Test
    void shouldCoverSchedulePeriodicCleanupAndSlowMetricBranches() throws Exception {
        Path scheduleDir = tempDir.resolve("schedule-periodic");
        StorageOptions options = StorageOptions.builder(scheduleDir)
                .backgroundCompactionEnabled(true)
                .slowQueryThresholdMillis(1L)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (SimpleKvEngine engine = SimpleKvEngine.open(options)) {
            setField(engine, "compactionRunning", Boolean.TRUE);
            invokePrivate(engine, "scheduleCompactionLocked", new Class<?>[]{boolean.class}, Boolean.TRUE);

            setField(engine, "compactionRunning", Boolean.FALSE);
            setField(engine, "closed", Boolean.TRUE);
            invokePrivate(engine, "scheduleCompactionLocked", new Class<?>[]{boolean.class}, Boolean.TRUE);
            waitForCompactionIdle(engine);

            setField(engine, "closed", Boolean.FALSE);
            SstableReader badReader = createReaderWithNonEmptyDirectory(tempDir.resolve("bad-schedule-reader"), 401L);
            setField(engine, "l0Readers", new ArrayList<SstableReader>(Collections.singletonList(badReader)));
            setField(engine, "l1Readers", new ArrayList<SstableReader>());
            ManifestState state = (ManifestState) getField(engine, "manifestState");
            state.getFiles().clear();
            state.getFiles().add(badReader.metadata());
            invokePrivate(engine, "scheduleCompactionLocked", new Class<?>[]{boolean.class}, Boolean.TRUE);
            waitForCompactionIdle(engine);

            setField(engine, "l0Readers", new ArrayList<SstableReader>());
            setField(engine, "l1Readers", new ArrayList<SstableReader>());
            invokePrivate(engine, "compactLocked", new Class<?>[]{boolean.class}, Boolean.FALSE);

            setField(engine, "closed", Boolean.TRUE);
            invokePrivate(engine, "runPeriodicCleanup", new Class<?>[0]);

            setField(engine, "closed", Boolean.FALSE);
            invokePrivate(engine, "runPeriodicCleanup", new Class<?>[0]);

            invokePrivate(engine, "maybeRecordSlowRead", new Class<?>[]{String.class, long.class}, "slow-key", 0L);
            invokePrivate(engine, "maybeRecordSlowScan",
                    new Class<?>[]{String.class, String.class, int.class, long.class},
                    "a", "z", 10, 0L);

            EngineStats stats = engine.stats();
            assertTrue(stats.getSlowReads() >= 1);
            assertTrue(stats.getSlowScans() >= 1);
        }

        Path periodicErrorDir = tempDir.resolve("periodic-error");
        StorageOptions errorOptions = StorageOptions.builder(periodicErrorDir)
                .backgroundCompactionEnabled(false)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        try (SimpleKvEngine engine = SimpleKvEngine.open(errorOptions)) {
            SstableReader badReader = createReaderWithNonEmptyDirectory(tempDir.resolve("bad-periodic-reader"), 501L);
            setField(engine, "l0Readers", new ArrayList<SstableReader>(Collections.singletonList(badReader)));
            setField(engine, "l1Readers", new ArrayList<SstableReader>());
            ManifestState state = (ManifestState) getField(engine, "manifestState");
            state.getFiles().clear();
            state.getFiles().add(badReader.metadata());

            invokePrivate(engine, "runPeriodicCleanup", new Class<?>[0]);
        }
    }

    private StorageOptions backpressureOptions(Path dataDir) {
        return StorageOptions.builder(dataDir)
                .mutableMemtableMaxEntries(1)
                .dataBlockMaxEntries(4)
                .l0CompactionTrigger(100)
                .l0StopWritesTrigger(2)
                .backpressureSleepMillis(1L)
                .backpressureMaxRetries(1)
                .backgroundCompactionEnabled(true)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();
    }

    private static void waitForCompactionIdle(SimpleKvEngine engine) throws Exception {
        for (int i = 0; i < 200; i++) {
            boolean running = (Boolean) getField(engine, "compactionRunning");
            if (!running) {
                return;
            }
            Thread.sleep(10L);
        }
    }

    private static SstableReader createReaderWithNonEmptyDirectory(Path directory, long fileId) throws Exception {
        Files.createDirectories(directory);
        Files.write(directory.resolve("keep.txt"), bytes("x"));

        SstableMetadata metadata = new SstableMetadata(
                fileId,
                0,
                directory.getFileName().toString(),
                "a",
                "a",
                null,
                1L,
                1L,
                1L
        );

        return newFakeReader(
                directory,
                metadata,
                Collections.singletonList(entry("a", 1L, ValueType.PUT, "v", -1L))
        );
    }

    private static SstableReader newFakeReader(Path path, SstableMetadata metadata, List<InternalEntry> entries) throws Exception {
        Constructor<SstableReader> ctor = SstableReader.class.getDeclaredConstructor(
                Path.class,
                SstableMetadata.class,
                List.class,
                SimpleBloomFilter.class
        );
        ctor.setAccessible(true);
        return ctor.newInstance(path, metadata, entries, null);
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] parameterTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static InternalEntry entry(String key, long sequence, ValueType type, String value, long expireAtMillis) {
        byte[] bytes = value == null ? null : value.getBytes(StandardCharsets.UTF_8);
        return new InternalEntry(new InternalKey(key, sequence, type), new InternalValue(bytes, expireAtMillis));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
