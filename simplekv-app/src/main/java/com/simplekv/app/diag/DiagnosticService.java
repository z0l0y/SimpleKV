package com.simplekv.app.diag;

import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.manifest.ManifestState;
import com.simplekv.storage.manifest.ManifestStore;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.sstable.SstableReader;
import com.simplekv.storage.wal.WalReader;
import com.simplekv.storage.wal.WalRecord;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DiagnosticService {
    private final Path dataDir;

    public DiagnosticService(SimpleKvStorageProperties properties) {
        this.dataDir = properties.getDataDir();
    }

    public List<Map<String, Object>> walTail(int limit) throws IOException {
        Path walFile = StorageLayout.walFile(dataDir);
        List<WalRecord> records = WalReader.readAll(walFile);
        int safeLimit = Math.max(0, limit);
        int start = Math.max(0, records.size() - safeLimit);

        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        for (int i = start; i < records.size(); i++) {
            WalRecord record = records.get(i);
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("sequence", Long.valueOf(record.sequence()));
            row.put("type", record.valueType().name());
            row.put("key", record.key());
            row.put("valueSize", Integer.valueOf(record.value() == null ? 0 : record.value().length));
            row.put("expireAtMillis", Long.valueOf(record.expireAtMillis()));
            result.add(row);
        }
        return result;
    }

    public Map<String, Object> manifestInspect() throws IOException {
        ManifestState state = new ManifestStore(dataDir).loadOrCreate();
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("nextFileId", Long.valueOf(state.getNextFileId()));
        result.put("lastFlushedSequence", Long.valueOf(state.getLastFlushedSequence()));
        result.put("fileCount", Integer.valueOf(state.getFiles().size()));

        List<Map<String, Object>> files = new ArrayList<Map<String, Object>>();
        for (SstableMetadata metadata : state.getFiles()) {
            files.add(sstableMeta(metadata, Files.exists(dataDir.resolve(metadata.getFileName()))));
        }
        result.put("files", files);
        return result;
    }

    public List<Map<String, Object>> sstInspect() throws IOException {
        ManifestState state = new ManifestStore(dataDir).loadOrCreate();
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

        for (SstableMetadata metadata : state.getFiles()) {
            Path sstablePath = dataDir.resolve(metadata.getFileName());
            Map<String, Object> row = sstableMeta(metadata, Files.exists(sstablePath));
            if (Files.exists(sstablePath)) {
                SstableReader reader = SstableReader.open(sstablePath, metadata);
                row.put("parsedEntryCount", Integer.valueOf(reader.allEntries().size()));
                row.put("hasBloomFilter", Boolean.valueOf(reader.hasBloomFilter()));
            }
            result.add(row);
        }
        return result;
    }

    public Path dataDir() {
        return dataDir;
    }

    private Map<String, Object> sstableMeta(SstableMetadata metadata, boolean exists) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("fileId", Long.valueOf(metadata.getFileId()));
        row.put("level", Integer.valueOf(metadata.getLevel()));
        row.put("fileName", metadata.getFileName());
        row.put("minKey", metadata.getMinKey());
        row.put("maxKey", metadata.getMaxKey());
        row.put("entryCount", Long.valueOf(metadata.getEntryCount()));
        row.put("exists", Boolean.valueOf(exists));
        return row;
    }
}
