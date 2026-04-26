package com.simplekv.storage.manifest;

import com.simplekv.storage.StorageLayout;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManifestStoreTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldCreateAndPersistManifestWithCurrentPointer() throws Exception {
        ManifestStore store = new ManifestStore(tempDir);
        ManifestState state = store.loadOrCreate();

        assertNotNull(state);
        assertTrue(Files.exists(StorageLayout.currentFile(tempDir)));
        assertTrue(Files.exists(StorageLayout.manifestFile(tempDir)));

        state.setNextFileId(9L);
        state.setLastFlushedSequence(100L);

        SstableMetadata meta = new SstableMetadata();
        meta.setFileId(1L);
        meta.setLevel(0);
        meta.setFileName("sst-1-L0.sst");
        meta.setMinKey("a");
        meta.setMaxKey("z");
        meta.setBloomFileName("sst-1-L0.sst.bf");
        meta.setMinSequence(1L);
        meta.setMaxSequence(99L);
        meta.setEntryCount(77L);

        state.setFiles(Arrays.asList(meta));
        store.save(state);

        ManifestState restored = store.loadOrCreate();
        assertEquals(9L, restored.getNextFileId());
        assertEquals(100L, restored.getLastFlushedSequence());
        assertEquals(1, restored.getFiles().size());
        assertEquals("sst-1-L0.sst", restored.getFiles().get(0).getFileName());
        assertEquals("sst-1-L0.sst.bf", restored.getFiles().get(0).getBloomFileName());
    }

    @Test
    void shouldFallbackToDefaultManifestWhenCurrentIsEmpty() throws Exception {
        Files.createDirectories(tempDir);
        Files.write(StorageLayout.currentFile(tempDir), Arrays.asList(""));

        ManifestStore store = new ManifestStore(tempDir);
        ManifestState state = store.loadOrCreate();
        assertNotNull(state);
        assertTrue(Files.exists(StorageLayout.manifestFile(tempDir)));
    }

    @Test
    void shouldCoverManifestStateAndMetadataMutators() {
        ManifestState state = new ManifestState();
        state.setNextFileId(2L);
        state.setLastFlushedSequence(3L);

        SstableMetadata metadata = new SstableMetadata(
                1L,
                0,
                "f",
                "a",
                "z",
                "f.bf",
                1L,
                2L,
                3L
        );

        state.setFiles(Arrays.asList(metadata));

        assertEquals(2L, state.getNextFileId());
        assertEquals(3L, state.getLastFlushedSequence());
        assertEquals(1, state.getFiles().size());

        SstableMetadata m = state.getFiles().get(0);
        assertEquals(1L, m.getFileId());
        assertEquals(0, m.getLevel());
        assertEquals("f", m.getFileName());
        assertEquals("a", m.getMinKey());
        assertEquals("z", m.getMaxKey());
        assertEquals("f.bf", m.getBloomFileName());
        assertEquals(1L, m.getMinSequence());
        assertEquals(2L, m.getMaxSequence());
        assertEquals(3L, m.getEntryCount());

        m.setFileId(4L);
        m.setLevel(1);
        m.setFileName("g");
        m.setMinKey("b");
        m.setMaxKey("y");
        m.setBloomFileName("g.bf");
        m.setMinSequence(5L);
        m.setMaxSequence(6L);
        m.setEntryCount(7L);

        assertEquals(4L, m.getFileId());
        assertEquals(1, m.getLevel());
        assertEquals("g", m.getFileName());
        assertEquals("b", m.getMinKey());
        assertEquals("y", m.getMaxKey());
        assertEquals("g.bf", m.getBloomFileName());
        assertEquals(5L, m.getMinSequence());
        assertEquals(6L, m.getMaxSequence());
        assertEquals(7L, m.getEntryCount());
        assertFalse(m.getBloomFileName().isEmpty());
    }
}
