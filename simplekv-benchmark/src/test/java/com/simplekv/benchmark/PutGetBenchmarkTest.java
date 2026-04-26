package com.simplekv.benchmark;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class PutGetBenchmarkTest {

    @Test
    void shouldRunBenchmarkLifecycle() throws Exception {
        PutGetBenchmark benchmark = new PutGetBenchmark();
        benchmark.setup();
        try {
            byte[] value = benchmark.putThenGet();
            assertNotNull(value);
        } finally {
            benchmark.tearDown();
        }
    }

    @Test
    void shouldIgnoreDeleteFailuresDuringTearDown() throws Exception {
        PutGetBenchmark benchmark = new PutGetBenchmark();
        Path dataDir = Files.createTempDirectory("simplekv-bench-teardown-");
        Path lockedFile = dataDir.resolve("locked.tmp");
        Files.write(lockedFile, new byte[]{1});
        Files.setAttribute(lockedFile, "dos:readonly", true);

        setField(benchmark, "dataDir", dataDir);
        benchmark.tearDown();

        Files.setAttribute(lockedFile, "dos:readonly", false);
        Files.deleteIfExists(lockedFile);
        Files.deleteIfExists(dataDir);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
