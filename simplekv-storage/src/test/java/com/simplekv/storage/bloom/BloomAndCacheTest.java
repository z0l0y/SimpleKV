package com.simplekv.storage.bloom;

import com.simplekv.storage.cache.LruBlockCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomAndCacheTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldPutAndProbeBloomFilter() {
        SimpleBloomFilter filter = SimpleBloomFilter.create(10, 12);
        filter.put("k1");
        filter.put("k2");

        SimpleBloomFilter emptyFilter = SimpleBloomFilter.create(10, 12);

        assertTrue(filter.mightContain("k1"));
        assertTrue(filter.mightContain("k2"));
        assertFalse(emptyFilter.mightContain("k-missing"));
        assertTrue(filter.bitSize() > 0);
        assertTrue(filter.hashFunctions() > 0);
        assertNotNull(filter.bitSetBytes());
    }

    @Test
    void shouldRoundTripBloomCodec() throws Exception {
        Path file = tempDir.resolve("f.bf");
        SimpleBloomFilter source = SimpleBloomFilter.create(8, 10);
        source.put("alpha");
        source.put("beta");

        BloomFilterCodec.write(file, source);
        SimpleBloomFilter restored = BloomFilterCodec.read(file);

        assertTrue(restored.mightContain("alpha"));
        assertTrue(restored.mightContain("beta"));
    }

    @Test
    void shouldRejectInvalidBloomRestoreAndDecode() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> SimpleBloomFilter.restore(0, 1, new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> SimpleBloomFilter.restore(1, 0, new byte[0]));

        Path invalidHeader = tempDir.resolve("invalid-header.bf");
        ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 5);
        header.putInt(0x00000000);
        header.putInt(1);
        header.putInt(64);
        header.putInt(2);
        header.putInt(0);
        Files.write(invalidHeader, header.array());
        assertThrows(Exception.class, () -> BloomFilterCodec.read(invalidHeader));

        Path invalid = tempDir.resolve("invalid.bf");
        Files.write(invalid, new byte[]{1, 2, 3, 4});
        assertThrows(Exception.class, () -> BloomFilterCodec.read(invalid));
    }

    @Test
    void shouldInvokeBloomCodecPrivateConstructor() throws Exception {
        Constructor<BloomFilterCodec> ctor = BloomFilterCodec.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        ctor.newInstance();
    }

    @Test
    void shouldOperateLruBlockCache() {
        LruBlockCache<String, String> emptyCache = new LruBlockCache<String, String>(1);
        assertEquals(0.0d, emptyCache.hitRate());

        LruBlockCache<String, String> cache = new LruBlockCache<String, String>(2);
        cache.put("a", "1");
        cache.put("b", "2");

        assertEquals(true, cache.get("a").isPresent());
        assertEquals(false, cache.get("c").isPresent());
        assertTrue(cache.hits() >= 1);
        assertTrue(cache.misses() >= 1);

        cache.put("c", "3");
        assertEquals(2, cache.size());
        assertTrue(cache.hitRate() >= 0.0d);
        assertEquals(2, cache.maxEntries());

        cache.remove("a");
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    void shouldValidateLruCacheConstructor() {
        assertThrows(IllegalArgumentException.class, () -> new LruBlockCache<String, String>(0));
    }
}
