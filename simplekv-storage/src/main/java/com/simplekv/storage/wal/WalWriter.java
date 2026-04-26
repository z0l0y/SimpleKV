package com.simplekv.storage.wal;

import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.storage.io.CodecUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

public final class WalWriter implements Closeable {
    private static final int HEADER_BYTES = Integer.BYTES + Integer.BYTES;

    private final FileChannel channel;
    private final FsyncPolicy fsyncPolicy;
    private final long fsyncEveryMillis;
    private volatile long lastSyncAt;

    public WalWriter(Path walFile, FsyncPolicy fsyncPolicy, long fsyncEveryMillis) throws IOException {
        this.fsyncPolicy = fsyncPolicy;
        this.fsyncEveryMillis = fsyncEveryMillis;
        Files.createDirectories(walFile.getParent());
        this.channel = FileChannel.open(
                walFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        this.channel.position(channel.size());
        this.lastSyncAt = System.currentTimeMillis();
    }

    public synchronized void append(WalRecord record) throws IOException {
        appendBatch(Collections.singletonList(record));
    }

    public synchronized void appendBatch(List<WalRecord> records) throws IOException {
        if (records == null || records.isEmpty()) {
            return;
        }

        ByteBuffer payload = encodeBatchPayload(records);
        byte[] payloadBytes = payload.array();

        CRC32 crc32 = new CRC32();
        crc32.update(payloadBytes);
        int checksum = (int) crc32.getValue();

        ByteBuffer frame = ByteBuffer.allocate(HEADER_BYTES + payloadBytes.length);
        frame.putInt(payloadBytes.length);
        frame.putInt(checksum);
        frame.put(payloadBytes);
        frame.flip();

        while (frame.hasRemaining()) {
            channel.write(frame);
        }

        maybeSync(false);
    }

    public synchronized void sync() throws IOException {
        maybeSync(true);
    }

    public synchronized void truncate() throws IOException {
        channel.truncate(0L);
        channel.position(0L);
        lastSyncAt = System.currentTimeMillis();
    }

    private void maybeSync(boolean force) throws IOException {
        long now = System.currentTimeMillis();
        switch (fsyncPolicy) {
            case ALWAYS:
                channel.force(true);
                lastSyncAt = now;
                break;
            case EVERY_N_MILLIS:
                if (force || now - lastSyncAt >= fsyncEveryMillis) {
                    channel.force(true);
                    lastSyncAt = now;
                }
                break;
            case MANUAL:
                if (force) {
                    channel.force(true);
                    lastSyncAt = now;
                }
                break;
            default:
                break;
        }
    }

    private static ByteBuffer encodeBatchPayload(List<WalRecord> records) {
        List<byte[]> keyBytesList = new ArrayList<byte[]>(records.size());
        List<byte[]> valueBytesList = new ArrayList<byte[]>(records.size());

        int payloadSize = Integer.BYTES;
        for (WalRecord record : records) {
            byte[] keyBytes = CodecUtils.stringToBytes(record.key());
            byte[] valueBytes = record.value();
            keyBytesList.add(keyBytes);
            valueBytesList.add(valueBytes);

            payloadSize += Long.BYTES
                    + Byte.BYTES
                    + Long.BYTES
                    + CodecUtils.estimateByteArraySize(keyBytes)
                    + CodecUtils.estimateByteArraySize(valueBytes);
        }

        ByteBuffer buffer = ByteBuffer.allocate(payloadSize);
        buffer.putInt(records.size());
        for (int i = 0; i < records.size(); i++) {
            WalRecord record = records.get(i);
            buffer.putLong(record.sequence());
            buffer.put(record.valueType().code());
            buffer.putLong(record.expireAtMillis());
            CodecUtils.putByteArray(buffer, keyBytesList.get(i));
            CodecUtils.putByteArray(buffer, valueBytesList.get(i));
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public synchronized void close() throws IOException {
        channel.force(true);
        channel.close();
    }
}
