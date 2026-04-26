package com.simplekv.app.diag;

import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.manifest.ManifestState;
import com.simplekv.storage.manifest.ManifestStore;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.InternalKey;
import com.simplekv.storage.model.InternalValue;
import com.simplekv.storage.model.ValueType;
import com.simplekv.storage.sstable.SstableWriter;
import com.simplekv.storage.wal.WalRecord;
import com.simplekv.storage.wal.WalWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiagnosticServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldInspectWalManifestAndSst() throws Exception {
        WalWriter walWriter = new WalWriter(StorageLayout.walFile(tempDir), FsyncPolicy.MANUAL, 1L);
        walWriter.append(new WalRecord(1L, ValueType.PUT, "k1", "v1".getBytes(StandardCharsets.UTF_8), -1L));
        walWriter.append(new WalRecord(2L, ValueType.PUT, "k2", "v2".getBytes(StandardCharsets.UTF_8), -1L));
        walWriter.close();

        List<InternalEntry> entries = Arrays.asList(
                new InternalEntry(new InternalKey("a", 1L, ValueType.PUT), new InternalValue("x".getBytes(StandardCharsets.UTF_8), -1L)),
                new InternalEntry(new InternalKey("b", 2L, ValueType.PUT), new InternalValue("y".getBytes(StandardCharsets.UTF_8), -1L))
        );
        SstableMetadata metadata = new SstableWriter().write(tempDir, 1L, 0, entries, 8, 10);

        ManifestState state = new ManifestState();
        state.setNextFileId(2L);
        state.setLastFlushedSequence(2L);
        state.setFiles(Arrays.asList(metadata));
        new ManifestStore(tempDir).save(state);

        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        properties.setDataDir(tempDir);
        DiagnosticService service = new DiagnosticService(properties);

        List<Map<String, Object>> walTail = service.walTail(1);
        assertEquals(1, walTail.size());
        assertEquals("k2", walTail.get(0).get("key"));

        List<Map<String, Object>> walTailZero = service.walTail(0);
        assertTrue(walTailZero.isEmpty());

        Map<String, Object> manifest = service.manifestInspect();
        assertEquals(2L, manifest.get("nextFileId"));
        assertEquals(2L, manifest.get("lastFlushedSequence"));
        assertEquals(1, manifest.get("fileCount"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> files = (List<Map<String, Object>>) manifest.get("files");
        assertEquals(1, files.size());
        assertTrue((Boolean) files.get(0).get("exists"));

        List<Map<String, Object>> sst = service.sstInspect();
        assertEquals(1, sst.size());
        assertEquals(2, sst.get(0).get("parsedEntryCount"));
        assertTrue((Boolean) sst.get(0).get("hasBloomFilter"));

        assertEquals(tempDir, service.dataDir());
    }

    @Test
    void shouldReportMissingSstable() throws Exception {
        ManifestState state = new ManifestState();
        state.setFiles(Arrays.asList(new SstableMetadata(99L, 1, "missing.sst", "a", "z", null, 1L, 2L, 3L)));
        new ManifestStore(tempDir).save(state);

        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        properties.setDataDir(tempDir);
        DiagnosticService service = new DiagnosticService(properties);

        List<Map<String, Object>> sst = service.sstInspect();
        assertEquals(1, sst.size());
        assertFalse((Boolean) sst.get(0).get("exists"));
    }
}
