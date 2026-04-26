package com.simplekv.storage.bloom;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class BloomFilterCodec {
    private static final int MAGIC = 0x534B5642;
    private static final int VERSION = 1;

    private BloomFilterCodec() {
    }

    public static void write(Path file, SimpleBloomFilter bloomFilter) throws IOException {
        byte[] data = bloomFilter.bitSetBytes();
        ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 5);
        header.putInt(MAGIC);
        header.putInt(VERSION);
        header.putInt(bloomFilter.bitSize());
        header.putInt(bloomFilter.hashFunctions());
        header.putInt(data.length);
        header.flip();

        Files.createDirectories(file.getParent());
        try (FileChannel channel = FileChannel.open(
                file,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )) {
            writeFully(channel, header);
            writeFully(channel, ByteBuffer.wrap(data));
        }
    }

    public static SimpleBloomFilter read(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 5);
            readFully(channel, header);
            header.flip();

            int magic = header.getInt();
            int version = header.getInt();
            int bitSize = header.getInt();
            int hashFunctions = header.getInt();
            int length = header.getInt();

            if (magic != MAGIC || version != VERSION || bitSize < 1 || hashFunctions < 1 || length < 0) {
                throw new IOException("Invalid bloom filter file: " + file);
            }

            ByteBuffer body = ByteBuffer.allocate(length);
            readFully(channel, body);
            body.flip();
            return SimpleBloomFilter.restore(bitSize, hashFunctions, body.array());
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("Unexpected EOF while reading bloom filter file");
            }
        }
    }
}
