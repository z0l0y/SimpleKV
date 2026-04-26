package com.simplekv.api.store;

import com.simplekv.api.model.EngineStats;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.model.Snapshot;
import com.simplekv.api.write.WriteBatch;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface KeyValueStore extends AutoCloseable {
    void put(String key, byte[] value) throws IOException;

    void ttlPut(String key, byte[] value, long expireAtMillis) throws IOException;

    Optional<byte[]> get(String key) throws IOException;

    Optional<byte[]> get(String key, Snapshot snapshot) throws IOException;

    void delete(String key) throws IOException;

    List<KeyValue> scan(String startKey, String endKey, int limit) throws IOException;

    List<KeyValue> scan(String startKey, String endKey, int limit, Snapshot snapshot) throws IOException;

    List<KeyValue> prefixScan(String prefix, int limit) throws IOException;

    void writeBatch(WriteBatch batch) throws IOException;

    Snapshot snapshot();

    EngineStats stats();

    String statsCommand();

    String sstDump();

    void flush() throws IOException;

    @Override
    void close() throws IOException;
}
