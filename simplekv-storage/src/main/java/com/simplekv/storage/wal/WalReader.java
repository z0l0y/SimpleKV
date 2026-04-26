package com.simplekv.storage.wal;

import com.simplekv.storage.io.CodecUtils;
import com.simplekv.storage.model.ValueType;

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

public final class WalReader {
    private static final int HEADER_BYTES = Integer.BYTES + Integer.BYTES;
    private static final int MAX_RECORD_BYTES = 32 * 1024 * 1024;

    private WalReader() {
    }

    public static List<WalRecord> readAll(Path walFile) throws IOException {
        if (!Files.exists(walFile)) {
            return Collections.emptyList();
        }

        List<WalRecord> records = new ArrayList<>();
        try (FileChannel channel = FileChannel.open(walFile, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);

            while (true) {
                header.clear();
                int headerRead = readFully(channel, header);
                if (headerRead == -1) {
                    break;
                }
                if (headerRead != HEADER_BYTES) {
                    break;
                }
                header.flip();

                int payloadLength = header.getInt();
                int checksum = header.getInt();

                if (payloadLength <= 0 || payloadLength > MAX_RECORD_BYTES) {
                    break;
                }

                ByteBuffer payload = ByteBuffer.allocate(payloadLength);
                int payloadRead = readFully(channel, payload);
                if (payloadRead != payloadLength) {
                    break;
                }
                byte[] payloadBytes = payload.array();

                CRC32 crc32 = new CRC32();
                crc32.update(payloadBytes);
                if ((int) crc32.getValue() != checksum) {
                    break;
                }

                records.addAll(decodeBatchPayload(ByteBuffer.wrap(payloadBytes)));
            }
        }
        return records;
    }

    private static List<WalRecord> decodeBatchPayload(ByteBuffer buffer) {
        int recordCount = buffer.getInt();
        if (recordCount <= 0) {
            return Collections.emptyList();
        }

        List<WalRecord> records = new ArrayList<WalRecord>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            long sequence = buffer.getLong();
            ValueType valueType = ValueType.fromCode(buffer.get());
            long expireAt = buffer.getLong();
            String key = CodecUtils.bytesToString(CodecUtils.getByteArray(buffer));
            byte[] value = CodecUtils.getByteArray(buffer);
            records.add(new WalRecord(sequence, valueType, key, value, expireAt));
        }
        return records;
    }

    private static int readFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                return total == 0 ? -1 : total;
            }
            total += read;
        }
        return total;
    }
}
