package com.simplekv.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SimpleKvNativeApplicationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldResolveDataDirFromGrandParentWhenRunningInTargetLikeDirectory() throws Exception {
        Path workingDirectory = tempDir.resolve("simplekv-app").resolve("target");
        Files.createDirectories(workingDirectory);

        Path projectDataDir = tempDir.resolve("data");
        Files.createDirectories(projectDataDir);
        Files.write(projectDataDir.resolve("MANIFEST.json"), "{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Files.write(projectDataDir.resolve("CURRENT"), "MANIFEST.json".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        Path resolved = SimpleKvNativeApplication.resolveDefaultDataDir(workingDirectory);
        assertEquals(projectDataDir.normalize(), resolved);
    }

    @Test
    void shouldFallbackToWorkingDirectoryDataWhenNoSimpleKvMarkersFound() throws Exception {
        Path workingDirectory = tempDir.resolve("sandbox");
        Files.createDirectories(workingDirectory);

        Path resolved = SimpleKvNativeApplication.resolveDefaultDataDir(workingDirectory);
        assertEquals(workingDirectory.resolve("data").normalize(), resolved);
    }
}
