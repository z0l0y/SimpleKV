package com.simplekv.storage.sstable;

import com.simplekv.storage.StorageLayout;
import com.simplekv.storage.bloom.BloomFilterCodec;
import com.simplekv.storage.bloom.SimpleBloomFilter;
import com.simplekv.storage.io.CodecUtils;
import com.simplekv.storage.manifest.SstableMetadata;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.InternalKey;
import com.simplekv.storage.model.InternalValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class SstableWriter {
    private static final class IndexEntry {
        private final String firstKey;
        private final long firstSequence;
        private final long blockOffset;

        private IndexEntry(String firstKey, long firstSequence, long blockOffset) {
            this.firstKey = firstKey;
            this.firstSequence = firstSequence;
            this.blockOffset = blockOffset;
        }

        public String firstKey() {
            return firstKey;
        }

        public long firstSequence() {
            return firstSequence;
        }

        public long blockOffset() {
            return blockOffset;
        }
    }

    public SstableMetadata write(
            Path dataDir,
            long fileId,
            int level,
            List<InternalEntry> entries,
            int dataBlockMaxEntries,
            int bloomFilterBitsPerKey
    ) throws IOException {
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("entries must not be empty");
        }

        List<InternalEntry> sorted = new ArrayList<>(entries);
        sorted.sort(Comparator.comparing(InternalEntry::key));

        Path sstableFile = StorageLayout.sstableFile(dataDir, fileId, level);
        Files.createDirectories(dataDir);

        try (FileChannel channel = FileChannel.open(
                sstableFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )) {
            writeInt(channel, SstableLayout.MAGIC);
            writeInt(channel, SstableLayout.VERSION);

            List<IndexEntry> indexEntries = new ArrayList<>();
            int blockCount = 0;

            int cursor = 0;
            while (cursor < sorted.size()) {
                int next = Math.min(sorted.size(), cursor + dataBlockMaxEntries);
                List<InternalEntry> block = sorted.subList(cursor, next);
                long blockOffset = channel.position();
                indexEntries.add(new IndexEntry(block.get(0).key().userKey(), block.get(0).key().sequence(), blockOffset));

                writeInt(channel, block.size());
                for (InternalEntry entry : block) {
                    writeInternalEntry(channel, entry);
                }
                cursor = next;
                blockCount++;
            }

            long indexOffset = channel.position();
            writeInt(channel, indexEntries.size());
            for (IndexEntry indexEntry : indexEntries) {
                writeString(channel, indexEntry.firstKey());
                writeLong(channel, indexEntry.firstSequence());
                writeLong(channel, indexEntry.blockOffset());
            }
            long indexLength = channel.position() - indexOffset;

            writeLong(channel, indexOffset);
            writeInt(channel, (int) indexLength);
            writeInt(channel, blockCount);
            writeLong(channel, sorted.size());
            writeInt(channel, SstableLayout.FOOTER_MAGIC);
        }

        Path bloomFile = StorageLayout.bloomFileForSstable(sstableFile);
        SimpleBloomFilter bloomFilter = SimpleBloomFilter.create(sorted.size(), bloomFilterBitsPerKey);
        for (InternalEntry entry : sorted) {
            bloomFilter.put(entry.key().userKey());
        }
        BloomFilterCodec.write(bloomFile, bloomFilter);

        InternalKey first = sorted.get(0).key();
        InternalKey last = sorted.get(sorted.size() - 1).key();
        long minSequence = sorted.stream().mapToLong(e -> e.key().sequence()).min().orElse(0L);
        long maxSequence = sorted.stream().mapToLong(e -> e.key().sequence()).max().orElse(0L);
        String sstableFileName = fileNameOf(sstableFile, "sstable file");
        String bloomFileName = fileNameOf(bloomFile, "bloom file");

        return new SstableMetadata(
                fileId,
                level,
            sstableFileName,
                first.userKey(),
                last.userKey(),
            bloomFileName,
                minSequence,
                maxSequence,
                sorted.size()
        );
    }

    private void writeInternalEntry(FileChannel channel, InternalEntry entry) throws IOException {
        InternalKey key = entry.key();
        InternalValue value = entry.value();

        writeString(channel, key.userKey());
        writeLong(channel, key.sequence());
        writeByte(channel, key.valueType().code());
        writeLong(channel, value.expireAtMillis());
        writeByteArray(channel, value.value());
    }

    private void writeString(FileChannel channel, String value) throws IOException {
        writeByteArray(channel, CodecUtils.stringToBytes(value));
    }

    private void writeByteArray(FileChannel channel, byte[] value) throws IOException {
        if (value == null) {
            writeInt(channel, -1);
            return;
        }
        writeInt(channel, value.length);
        writeBytes(channel, value);
    }

    private void writeInt(FileChannel channel, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(value).flip();
        writeFully(channel, buffer);
    }

    private void writeLong(FileChannel channel, long value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value).flip();
        writeFully(channel, buffer);
    }

    private void writeByte(FileChannel channel, byte value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(value).flip();
        writeFully(channel, buffer);
    }

    private void writeBytes(FileChannel channel, byte[] value) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        writeFully(channel, buffer);
    }

    private void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static String fileNameOf(Path path, String description) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException(description + " must include a file name");
        }
        return fileName.toString();
    }
}
