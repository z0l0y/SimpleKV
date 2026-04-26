package com.simplekv.benchmark;

import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.core.SimpleKvEngines;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.UUID;

@State(Scope.Benchmark)
public class PutGetBenchmark {

    private KeyValueStore store;
    private Path dataDir;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dataDir = Paths.get(System.getProperty("java.io.tmpdir"), "simplekv-bench-" + UUID.randomUUID());
        StorageOptions options = StorageOptions.builder(dataDir)
                .mutableMemtableMaxEntries(1024)
                .dataBlockMaxEntries(128)
                .l0CompactionTrigger(4)
                .fsyncPolicy(FsyncPolicy.MANUAL)
                .build();
        store = SimpleKvEngines.open(options);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (store != null) {
            store.close();
        }
        if (dataDir != null && Files.exists(dataDir)) {
            Files.walk(dataDir)
                    .sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
        }
    }

    @Benchmark
    public byte[] putThenGet() throws IOException {
        KeyValueStore localStore = Objects.requireNonNull(store, "Benchmark store is not initialized");
        String key = "k-" + System.nanoTime();
        byte[] value = ("v-" + key).getBytes(StandardCharsets.UTF_8);
        localStore.put(key, value);
        return localStore.get(key).orElse(null);
    }
}
