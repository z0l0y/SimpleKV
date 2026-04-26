package com.simplekv.storage.sstable;

import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.bloom.BloomFilterCodec;
import com.simplekv.storage.bloom.SimpleBloomFilter;
import com.simplekv.storage.io.CodecUtils;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.InternalKey;
import com.simplekv.storage.model.InternalValue;
import com.simplekv.storage.model.ValueType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class SstableReader {
    private final Path path;
    private final SstableMetadata metadata;
    private final List<InternalEntry> entries;
    private final Map<String, Integer> firstOffsetByUserKey;
    private final SimpleBloomFilter bloomFilter;

    private SstableReader(Path path, SstableMetadata metadata, List<InternalEntry> entries, SimpleBloomFilter bloomFilter) {
        this.path = path;
        this.metadata = metadata;
        this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
        this.bloomFilter = bloomFilter;
        this.firstOffsetByUserKey = new HashMap<>();
        for (int i = 0; i < this.entries.size(); i++) {
            firstOffsetByUserKey.putIfAbsent(this.entries.get(i).key().userKey(), i);
        }
    }

    public static SstableReader open(Path file, SstableMetadata metadata) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            int magic = readInt(channel);
            int version = readInt(channel);
            if (magic != SstableLayout.MAGIC || version != SstableLayout.VERSION) {
                throw new IOException("Invalid SSTable header for file: " + file);
            }

            long fileSize = channel.size();
            long footerPos = fileSize - SstableLayout.FOOTER_BYTES;
            if (footerPos <= SstableLayout.HEADER_BYTES) {
                throw new IOException("SSTable file too small: " + file);
            }

            channel.position(footerPos);
            long indexOffset = readLong(channel);
            int indexLength = readInt(channel);
            int blockCount = readInt(channel);
            long entryCount = readLong(channel);
            int footerMagic = readInt(channel);
            if (footerMagic != SstableLayout.FOOTER_MAGIC) {
                throw new IOException("Invalid SSTable footer for file: " + file);
            }

            if (indexOffset < SstableLayout.HEADER_BYTES || indexOffset >= footerPos || indexLength <= 0 || blockCount <= 0 || entryCount <= 0) {
                throw new IOException("Invalid SSTable offsets for file: " + file);
            }

            channel.position(SstableLayout.HEADER_BYTES);
            List<InternalEntry> entries = new ArrayList<>();
            while (channel.position() < indexOffset) {
                int blockEntries = readInt(channel);
                if (blockEntries <= 0) {
                    throw new IOException("Invalid block entry count in file: " + file);
                }
                for (int i = 0; i < blockEntries; i++) {
                    entries.add(readInternalEntry(channel));
                }
            }

            if (entries.isEmpty()) {
                throw new IOException("Empty SSTable data in file: " + file);
            }

            Collections.sort(entries, (a, b) -> a.key().compareTo(b.key()));

            SimpleBloomFilter bloomFilter = loadBloomFilter(file, metadata);
            return new SstableReader(file, metadata, entries, bloomFilter);
        }
    }

    public boolean hasBloomFilter() {
        return bloomFilter != null;
    }

    public boolean mightContain(String key) {
        if (bloomFilter == null) {
            return true;
        }
        return bloomFilter.mightContain(key);
    }

    public Path path() {
        return path;
    }

    public SstableMetadata metadata() {
        return new SstableMetadata(
                metadata.getFileId(),
                metadata.getLevel(),
                metadata.getFileName(),
                metadata.getMinKey(),
                metadata.getMaxKey(),
                metadata.getBloomFileName(),
                metadata.getMinSequence(),
                metadata.getMaxSequence(),
                metadata.getEntryCount()
        );
    }

    public List<InternalEntry> allEntries() {
        return entries;
    }

    public Optional<InternalEntry> latestEntry(String key, long visibleSequence) {
        Integer start = firstOffsetByUserKey.get(key);
        if (start == null) {
            return Optional.empty();
        }
        for (int i = start; i < entries.size(); i++) {
            InternalEntry entry = entries.get(i);
            InternalKey internalKey = entry.key();
            if (!internalKey.userKey().equals(key)) {
                break;
            }
            if (internalKey.sequence() > visibleSequence) {
                continue;
            }
            return Optional.of(entry);
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
        String seenKey = null;
        for (InternalEntry entry : entries) {
            String userKey = entry.key().userKey();
            if (userKey.compareTo(startKey) < 0) {
                continue;
            }
            if (userKey.compareTo(endKey) > 0) {
                break;
            }
            if (entry.key().sequence() > visibleSequence) {
                continue;
            }
            if (seenKey != null && seenKey.equals(userKey)) {
                continue;
            }
            seenKey = userKey;
            if (entry.key().valueType() == ValueType.DELETE || entry.value().isExpired(nowMillis)) {
                continue;
            }
            result.add(entry);
            if (result.size() >= limit) {
                break;
            }
        }
        return result;
    }

    private static InternalEntry readInternalEntry(FileChannel channel) throws IOException {
        String userKey = readString(channel);
        long sequence = readLong(channel);
        ValueType valueType = ValueType.fromCode(readByte(channel));
        long expireAtMillis = readLong(channel);
        byte[] value = readByteArray(channel);

        InternalKey key = new InternalKey(userKey, sequence, valueType);
        InternalValue internalValue = new InternalValue(value, expireAtMillis);
        return new InternalEntry(key, internalValue);
    }

    private static String readString(FileChannel channel) throws IOException {
        return CodecUtils.bytesToString(readByteArray(channel));
    }

    private static byte[] readByteArray(FileChannel channel) throws IOException {
        int length = readInt(channel);
        if (length < 0) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate(length);
        readFully(channel, buffer);
        return buffer.array();
    }

    private static byte readByte(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        readFully(channel, buffer);
        return buffer.get(0);
    }

    private static int readInt(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        readFully(channel, buffer);
        return buffer.getInt(0);
    }

    private static long readLong(FileChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        readFully(channel, buffer);
        return buffer.getLong(0);
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("Unexpected EOF while reading SSTable");
            }
        }
        buffer.flip();
    }

    private static SimpleBloomFilter loadBloomFilter(Path sstableFile, SstableMetadata metadata) {
        try {
            Path bloomFile;
            if (metadata.getBloomFileName() != null && !metadata.getBloomFileName().trim().isEmpty()) {
                bloomFile = sstableFile.resolveSibling(metadata.getBloomFileName());
            } else {
                bloomFile = StorageLayout.bloomFileForSstable(sstableFile);
            }
            if (!Files.exists(bloomFile)) {
                return null;
            }
            return BloomFilterCodec.read(bloomFile);
        } catch (IOException ex) {
            return null;
        }
    }
}
