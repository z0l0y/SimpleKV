package com.simplekv.storage.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class LruBlockCache<K, V> {
    private final int maxEntries;
    private final LinkedHashMap<K, V> cache;
    private long hits;
    private long misses;

    public LruBlockCache(final int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
        this.cache = new LinkedHashMap<K, V>(maxEntries, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return super.size() > LruBlockCache.this.maxEntries;
            }
        };
    }

    public synchronized void put(K key, V value) {
        cache.put(key, value);
    }

    public synchronized Optional<V> get(K key) {
        V value = cache.get(key);
        if (value == null) {
            misses++;
            return Optional.empty();
        }
        hits++;
        return Optional.of(value);
    }

    public synchronized void remove(K key) {
        cache.remove(key);
    }

    public synchronized void clear() {
        cache.clear();
    }

    public synchronized int size() {
        return cache.size();
    }

    public synchronized long hits() {
        return hits;
    }

    public synchronized long misses() {
        return misses;
    }

    public synchronized double hitRate() {
        long total = hits + misses;
        if (total == 0L) {
            return 0.0d;
        }
        return (double) hits / (double) total;
    }

    public int maxEntries() {
        return maxEntries;
    }
}
