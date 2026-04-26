package com.simplekv.storage.mem;

import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.InternalKey;
import com.simplekv.storage.model.InternalValue;
import com.simplekv.storage.model.ValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public final class MemTable {
    private final ConcurrentSkipListMap<InternalKey, InternalValue> map = new ConcurrentSkipListMap<>();

    public void put(long sequence, String key, byte[] value, long expireAtMillis) {
        map.put(new InternalKey(key, sequence, ValueType.PUT), new InternalValue(value, expireAtMillis));
    }

    public void delete(long sequence, String key) {
        map.put(new InternalKey(key, sequence, ValueType.DELETE), new InternalValue(null, -1L));
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public List<InternalEntry> snapshotEntries() {
        List<InternalEntry> list = new ArrayList<>(map.size());
        for (Map.Entry<InternalKey, InternalValue> entry : map.entrySet()) {
            list.add(new InternalEntry(entry.getKey(), entry.getValue()));
        }
        return list;
    }

    public Optional<InternalEntry> latestEntry(String key, long visibleSequence) {
        NavigableMap<InternalKey, InternalValue> tail = map.tailMap(InternalKey.seekKey(key), true);
        Iterator<Map.Entry<InternalKey, InternalValue>> iterator = tail.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<InternalKey, InternalValue> entry = iterator.next();
            InternalKey internalKey = entry.getKey();
            if (!internalKey.userKey().equals(key)) {
                break;
            }
            if (internalKey.sequence() > visibleSequence) {
                continue;
            }
            return Optional.of(new InternalEntry(internalKey, entry.getValue()));
        }
        return Optional.empty();
    }

    public Optional<InternalEntry> getVisible(String key, long visibleSequence, long nowMillis) {
        Optional<InternalEntry> latest = latestEntry(key, visibleSequence);
        if (!latest.isPresent()) {
            return Optional.empty();
        }

        InternalEntry entry = latest.get();
        if (entry.key().valueType() == ValueType.DELETE || entry.value().isExpired(nowMillis)) {
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    public List<InternalEntry> scanVisible(String startKey, String endKey, int limit, long visibleSequence, long nowMillis) {
        if (limit <= 0) {
            return Collections.emptyList();
        }
        List<InternalEntry> result = new ArrayList<>(Math.min(limit, 128));
        String currentKey = null;
        NavigableMap<InternalKey, InternalValue> tail = map.tailMap(InternalKey.seekKey(startKey), true);
        for (Map.Entry<InternalKey, InternalValue> entry : tail.entrySet()) {
            InternalKey internalKey = entry.getKey();
            if (internalKey.userKey().compareTo(endKey) > 0) {
                break;
            }
            if (internalKey.sequence() > visibleSequence) {
                continue;
            }
            if (currentKey != null && currentKey.equals(internalKey.userKey())) {
                continue;
            }
            currentKey = internalKey.userKey();
            if (internalKey.valueType() == ValueType.DELETE || entry.getValue().isExpired(nowMillis)) {
                continue;
            }
            result.add(new InternalEntry(internalKey, entry.getValue()));
            if (result.size() >= limit) {
                break;
            }
        }
        return result;
    }
}
