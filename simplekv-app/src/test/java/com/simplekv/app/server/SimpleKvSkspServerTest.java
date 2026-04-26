package com.simplekv.app.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekv.api.model.EngineStats;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.model.Snapshot;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.write.WriteBatch;
import com.simplekv.app.cli.SimpleKvCli;
import com.simplekv.app.config.SimpleKvServerProperties;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.app.diag.DiagnosticService;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SimpleKvSkspServerTest {

    @Test
    void shouldServeSkspCommands() throws Exception {
        SimpleKvStorageProperties storageProperties = new SimpleKvStorageProperties();
        storageProperties.setDataDir(java.nio.file.Paths.get("./server-test"));

        SimpleKvServerProperties serverProperties = new SimpleKvServerProperties();
        serverProperties.setHost("127.0.0.1");
        serverProperties.setPort(0);
        serverProperties.setBannerEnabled(false);

        TestStore store = new TestStore();
        DiagnosticService diagnosticService = mock(DiagnosticService.class);
        SimpleKvCli cli = new SimpleKvCli(store, diagnosticService, new ObjectMapper(), storageProperties);

        SimpleKvServerBanner banner = new SimpleKvServerBanner(serverProperties, storageProperties);
        SimpleKvSkspServer server = new SimpleKvSkspServer(cli, store, serverProperties, banner);

        AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread serverThread = Thread.startVirtualThread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start();
                }
                catch (Throwable ex) {
                    failure.set(ex);
                }
            }
        });

        assertTrue(server.awaitStarted(5, TimeUnit.SECONDS));

        try (Socket socket = new Socket("127.0.0.1", server.getBoundPort());
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {

            writer.write("put demo-key demo-value\n");
            writer.flush();
            Frame putFrame = readFrame(reader);
            assertEquals("SKSP/1.0 OK 0", putFrame.header);
            assertTrue(putFrame.payload.size() > 0);
            assertTrue(putFrame.payload.toString().contains("\"status\""));

            writer.write("get demo-key\n");
            writer.flush();
            Frame getFrame = readFrame(reader);
            assertEquals("SKSP/1.0 OK 0", getFrame.header);
            assertTrue(getFrame.payload.toString().contains("demo-value"));

            writer.write("ping\n");
            writer.flush();
            Frame pingFrame = readFrame(reader);
            assertEquals("SKSP/1.0 OK 0", pingFrame.header);
            assertEquals(List.of("PONG"), pingFrame.payload);

            writer.write("quit\n");
            writer.flush();
            Frame quitFrame = readFrame(reader);
            assertEquals("SKSP/1.0 OK 0", quitFrame.header);
            assertEquals(List.of("bye"), quitFrame.payload);
        }
        finally {
            server.close();
            serverThread.join(5000L);
        }

        assertNull(failure.get());
    }

    @Test
    void shouldServeRedisLikeCommands() throws Exception {
        SimpleKvStorageProperties storageProperties = new SimpleKvStorageProperties();
        storageProperties.setDataDir(java.nio.file.Paths.get("./server-test-redis"));

        SimpleKvServerProperties serverProperties = new SimpleKvServerProperties();
        serverProperties.setHost("127.0.0.1");
        serverProperties.setPort(0);
        serverProperties.setBannerEnabled(false);

        TestStore store = new TestStore();
        DiagnosticService diagnosticService = mock(DiagnosticService.class);
        SimpleKvCli cli = new SimpleKvCli(store, diagnosticService, new ObjectMapper(), storageProperties);

        SimpleKvServerBanner banner = new SimpleKvServerBanner(serverProperties, storageProperties);
        SimpleKvSkspServer server = new SimpleKvSkspServer(cli, store, serverProperties, banner);

        AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        Thread serverThread = Thread.startVirtualThread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.start();
                }
                catch (Throwable ex) {
                    failure.set(ex);
                }
            }
        });

        assertTrue(server.awaitStarted(5, TimeUnit.SECONDS));

        try (Socket socket = new Socket("127.0.0.1", server.getBoundPort());
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {

            writer.write("set user:1 alice\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("OK"));

            writer.write("get user:1\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("alice"));

            writer.write("exists user:1 missing\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("1"));

            writer.write("incr counter\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("1"));

            writer.write("decr counter\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("0"));

            writer.write("mset a 1 b 2\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("OK"));

            writer.write("mget a b c\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("1", "2", "(nil)"));

                writer.write("lpush list:1 a b\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("2"));

                writer.write("rpush list:1 c\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("3"));

                writer.write("lrange list:1 0 -1\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("b", "a", "c"));

                writer.write("llen list:1\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("3"));

                writer.write("lpop list:1\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("b"));

                writer.write("sadd set:1 red green red\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("2"));

                writer.write("scard set:1\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("2"));

                writer.write("sismember set:1 green\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("1"));

                writer.write("smembers set:1\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("green", "red"));

                writer.write("hset hash:1 name ming city shanghai\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("2"));

                writer.write("hget hash:1 name\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("ming"));

                writer.write("hexists hash:1 city\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("1"));

                writer.write("hlen hash:1\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 OK 0", List.of("2"));

                writer.write("hgetall hash:1\n");
                writer.flush();
                Frame hgetallFrame = readFrame(reader);
                assertEquals("SKSP/1.0 OK 0", hgetallFrame.header);
                assertEquals(4, hgetallFrame.payload.size());
                assertTrue(hgetallFrame.payload.contains("name"));
                assertTrue(hgetallFrame.payload.contains("ming"));

                writer.write("get list:1\n");
                writer.flush();
                assertFrame(reader, "SKSP/1.0 ERR 2",
                    List.of("WRONGTYPE Operation against a key holding the wrong kind of value"));

            writer.write("info\n");
            writer.flush();
            Frame infoFrame = readFrame(reader);
            assertEquals("SKSP/1.0 OK 0", infoFrame.header);
            assertTrue(infoFrame.payload.toString().contains("server:SimpleKV"));

            writer.write("del user:1\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("1"));

            writer.write("get user:1\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("(nil)"));

            writer.write("quit\n");
            writer.flush();
            assertFrame(reader, "SKSP/1.0 OK 0", List.of("bye"));
        }
        finally {
            server.close();
            serverThread.join(5000L);
        }

        assertNull(failure.get());
    }

    private static void assertFrame(BufferedReader reader, String header, List<String> payload) throws IOException {
        Frame frame = readFrame(reader);
        assertEquals(header, frame.header);
        assertEquals(payload, frame.payload);
    }

    private static Frame readFrame(BufferedReader reader) throws IOException {
        String header = reader.readLine();
        int payloadSize = Integer.parseInt(reader.readLine());
        List<String> payload = new ArrayList<String>();
        for (int i = 0; i < payloadSize; i++) {
            payload.add(reader.readLine());
        }
        return new Frame(header, payload);
    }

    private static final class Frame {
        private final String header;
        private final List<String> payload;

        private Frame(String header, List<String> payload) {
            this.header = header;
            this.payload = payload;
        }
    }

    private static final class TestStore implements KeyValueStore {
        private final TreeMap<String, byte[]> values = new TreeMap<String, byte[]>();

        @Override
        public void put(String key, byte[] value) {
            values.put(key, value);
        }

        @Override
        public void ttlPut(String key, byte[] value, long expireAtMillis) {
            values.put(key, value);
        }

        @Override
        public Optional<byte[]> get(String key) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public Optional<byte[]> get(String key, Snapshot snapshot) {
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public void delete(String key) {
            values.remove(key);
        }

        @Override
        public List<KeyValue> scan(String startKey, String endKey, int limit) {
            return scanInternal(startKey, endKey, limit);
        }

        @Override
        public List<KeyValue> scan(String startKey, String endKey, int limit, Snapshot snapshot) {
            return scanInternal(startKey, endKey, limit);
        }

        @Override
        public List<KeyValue> prefixScan(String prefix, int limit) {
            List<KeyValue> result = new ArrayList<KeyValue>();
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                if (!entry.getKey().startsWith(prefix)) {
                    continue;
                }
                result.add(new KeyValue(entry.getKey(), entry.getValue()));
                if (result.size() >= limit) {
                    break;
                }
            }
            return result;
        }

        @Override
        public void writeBatch(WriteBatch batch) {
            throw new UnsupportedOperationException("writeBatch is not needed for this test");
        }

        @Override
        public Snapshot snapshot() {
            return new Snapshot(1L);
        }

        @Override
        public EngineStats stats() {
            return new EngineStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, 0, 0.0d, 0.0d, 0.0d);
        }

        @Override
        public String statsCommand() {
            return "stats";
        }

        @Override
        public String sstDump() {
            return "[]";
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        private List<KeyValue> scanInternal(String startKey, String endKey, int limit) {
            List<KeyValue> result = new ArrayList<KeyValue>();
            for (Map.Entry<String, byte[]> entry : values.subMap(startKey, true, endKey, true).entrySet()) {
                result.add(new KeyValue(entry.getKey(), entry.getValue()));
                if (result.size() >= limit) {
                    break;
                }
            }
            return result;
        }
    }
}
