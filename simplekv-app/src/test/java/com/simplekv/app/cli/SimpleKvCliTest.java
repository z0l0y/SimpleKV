package com.simplekv.app.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekv.api.model.EngineStats;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.model.Snapshot;
import com.simplekv.api.write.WriteBatch;
import com.simplekv.api.write.WriteOperation;
import com.simplekv.api.write.WriteOperationType;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.app.diag.DiagnosticService;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleKvCliTest {

    @Test
    void shouldExecuteAllCommands() {
        TestStore store = new TestStore();
        store.putLocal("a", "1");
        store.putLocal("ab", "2");
        store.putLocal("n", null);

        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        properties.setDataDir(Paths.get("./cli-test"));
        properties.setScanDefaultLimit(10);

        FakeDiagnosticService diagnostic = new FakeDiagnosticService(properties);
        SimpleKvCli cli = new SimpleKvCli(store, diagnostic, new ObjectMapper(), properties);

        assertEquals(0, run(cli, "put", "k", "v").exitCode);
        assertEquals(0, run(cli, "ttl-put", "ttl", "v", "123").exitCode);
        assertEquals(0, run(cli, "get", "k").exitCode);
        assertEquals(0, run(cli, "delete", "k").exitCode);
        assertEquals(0, run(cli, "scan", "a", "z").exitCode);
        assertEquals(0, run(cli, "scan", "a", "z", "10").exitCode);
        assertEquals(0, run(cli, "prefix", "a").exitCode);
        assertEquals(0, run(cli, "prefix", "a", "10").exitCode);
        assertEquals(0, run(cli, "stats").exitCode);
        assertEquals(0, run(cli, "sst-dump").exitCode);
        assertEquals(0, run(cli, "recover").exitCode);
        assertEquals(0, run(cli, "compact").exitCode);
        assertEquals(0, run(cli, "trace-key", "ab").exitCode);
        assertEquals(0, run(cli, "wal-tail").exitCode);
        assertEquals(0, run(cli, "wal-tail", "1").exitCode);
        assertEquals(0, run(cli, "sst-inspect").exitCode);
        assertEquals(0, run(cli, "manifest-inspect").exitCode);
        assertEquals(0, run(cli, "lpush", "list:1", "a", "b").exitCode);
        assertEquals(0, run(cli, "rpush", "list:1", "c").exitCode);
        assertEquals(0, run(cli, "lrange", "list:1", "0", "-1").exitCode);
        assertEquals(0, run(cli, "llen", "list:1").exitCode);
        assertEquals(0, run(cli, "lpop", "list:1").exitCode);
        assertEquals(0, run(cli, "rpop", "list:1").exitCode);
        assertEquals(0, run(cli, "sadd", "set:1", "red", "green", "red").exitCode);
        assertEquals(0, run(cli, "smembers", "set:1").exitCode);
        assertEquals(0, run(cli, "sismember", "set:1", "green").exitCode);
        assertEquals(0, run(cli, "scard", "set:1").exitCode);
        assertEquals(0, run(cli, "srem", "set:1", "red").exitCode);
        assertEquals(0, run(cli, "hset", "hash:1", "name", "ming", "city", "shanghai").exitCode);
        assertEquals(0, run(cli, "hget", "hash:1", "name").exitCode);
        assertEquals(0, run(cli, "hexists", "hash:1", "city").exitCode);
        assertEquals(0, run(cli, "hlen", "hash:1").exitCode);
        assertEquals(0, run(cli, "hgetall", "hash:1").exitCode);
        assertEquals(0, run(cli, "hdel", "hash:1", "city").exitCode);

        RunResult jsonResult = run(cli, "--json", "stats");
        assertEquals(0, jsonResult.exitCode);
        assertTrue(jsonResult.output.contains("\"summary\""));
    }

    @Test
    void shouldHandleHelpAndErrorPaths() {
        TestStore store = new TestStore();
        store.failOnPut = true;

        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        FakeDiagnosticService diagnostic = new FakeDiagnosticService(properties);
        SimpleKvCli cli = new SimpleKvCli(store, diagnostic, new ObjectMapper(), properties);

        assertEquals(0, run(cli).exitCode);
        assertEquals(0, runNull(cli).exitCode);
        assertEquals(0, run(cli, "--json").exitCode);

        RunResult help = run(cli);
        assertTrue(help.output.contains("Key-Value Operations:"));
        assertTrue(help.output.contains("Collection Operations:"));
        assertTrue(help.output.contains("Diagnostic Commands:"));
        assertTrue(help.output.contains("Use \"simplekv help <command>\" for more information about a command."));

        RunResult helpTopic = run(cli, "help", "lpush");
        assertEquals(0, helpTopic.exitCode);
        assertTrue(helpTopic.output.contains("Command: lpush"));
        assertTrue(helpTopic.output.contains("Push values to the left of a list."));

        RunResult unknownTopic = run(cli, "help", "missing-topic");
        assertEquals(0, unknownTopic.exitCode);
        assertTrue(unknownTopic.output.contains("Unknown command or topic: missing-topic"));

        RunResult invalid = run(cli, "unknown");
        assertEquals(2, invalid.exitCode);
        assertTrue(invalid.output.contains("usage"));

        assertEquals(2, run(cli, "get").exitCode);
        assertEquals(2, run(cli, "ttl-put", "k", "v", "bad").exitCode);

        RunResult ioFailure = run(cli, "put", "k", "v");
        assertEquals(1, ioFailure.exitCode);
        assertTrue(ioFailure.output.contains("io error"));
    }

    @Test
    void shouldSupportInteractiveRepl() {
        TestStore store = new TestStore();
        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        FakeDiagnosticService diagnostic = new FakeDiagnosticService(properties);
        SimpleKvCli cli = new SimpleKvCli(store, diagnostic, new ObjectMapper(), properties);

        String script = "put demo-key \"demo value\"\n"
                + "help lpush\n"
                + "simplekv help put\n"
                + "kv help sadd\n"
                + "simplekv get demo-key\n"
                + "kv put test-key test-value\n"
                + "get demo-key\n"
                + "quit\n";
        InputStream in = new ByteArrayInputStream(script.getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int code = cli.run(new String[]{"repl"}, in, new PrintStream(output));
        String text = new String(output.toByteArray(), StandardCharsets.UTF_8);

        assertEquals(0, code);
        assertTrue(text.contains("Connected to SimpleKV interactive shell."));
        assertTrue(text.contains("Command: lpush"));
        assertTrue(text.contains("Command: put"));
        assertTrue(text.contains("Command: sadd"));
        assertTrue(text.contains("\"key\" : \"demo-key\""));
        assertTrue(text.contains("\"value\" : \"demo value\""));
        assertTrue(text.contains("\"status\" : \"ok\""));
        assertTrue(text.contains("bye"));
    }

    private static RunResult run(SimpleKvCli cli, String... args) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int code = cli.run(args, new PrintStream(output));
        return new RunResult(code, new String(output.toByteArray(), StandardCharsets.UTF_8));
    }

    private static RunResult runNull(SimpleKvCli cli) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int code = cli.run(null, new PrintStream(output));
        return new RunResult(code, new String(output.toByteArray(), StandardCharsets.UTF_8));
    }

    private static final class RunResult {
        private final int exitCode;
        private final String output;

        private RunResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }

    private static final class TestStore implements KeyValueStore {
        private final TreeMap<String, byte[]> values = new TreeMap<String, byte[]>();
        private long writes;
        private long reads;
        private long deletes;
        private long scans;
        private long flushes;
        private boolean failOnPut;

        void putLocal(String key, String value) {
            values.put(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void put(String key, byte[] value) throws IOException {
            if (failOnPut) {
                throw new IOException("boom");
            }
            values.put(key, value);
            writes++;
        }

        @Override
        public void ttlPut(String key, byte[] value, long expireAtMillis) {
            values.put(key, value);
            writes++;
        }

        @Override
        public Optional<byte[]> get(String key) {
            reads++;
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public Optional<byte[]> get(String key, Snapshot snapshot) {
            reads++;
            return Optional.ofNullable(values.get(key));
        }

        @Override
        public void delete(String key) {
            values.remove(key);
            deletes++;
        }

        @Override
        public List<KeyValue> scan(String startKey, String endKey, int limit) {
            scans++;
            return scanInternal(startKey, endKey, limit);
        }

        @Override
        public List<KeyValue> scan(String startKey, String endKey, int limit, Snapshot snapshot) {
            scans++;
            return scanInternal(startKey, endKey, limit);
        }

        @Override
        public List<KeyValue> prefixScan(String prefix, int limit) {
            scans++;
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
            for (WriteOperation operation : batch.operations()) {
                if (operation.type() == WriteOperationType.PUT) {
                    values.put(operation.key(), operation.value());
                } else {
                    values.remove(operation.key());
                }
            }
            writes += batch.size();
        }

        @Override
        public Snapshot snapshot() {
            return new Snapshot(1L);
        }

        @Override
        public EngineStats stats() {
            return new EngineStats(
                    writes,
                    reads,
                    deletes,
                    scans,
                    flushes,
                    1,
                    1,
                    1,
                    1,
                    1,
                    0,
                    0,
                    0,
                    values.size(),
                    0,
                    1,
                    1,
                    1.0,
                    1.0,
                    1.0
            );
        }

        @Override
        public String statsCommand() {
            return "stats";
        }

        @Override
        public String sstDump() {
            return "sst";
        }

        @Override
        public void flush() {
            flushes++;
        }

        @Override
        public void close() {
        }

        private List<KeyValue> scanInternal(String startKey, String endKey, int limit) {
            List<KeyValue> result = new ArrayList<KeyValue>();
            for (Map.Entry<String, byte[]> entry : values.entrySet()) {
                if (entry.getKey().compareTo(startKey) < 0) {
                    continue;
                }
                if (entry.getKey().compareTo(endKey) > 0) {
                    break;
                }
                result.add(new KeyValue(entry.getKey(), entry.getValue()));
                if (result.size() >= limit) {
                    break;
                }
            }
            return result;
        }
    }

    private static final class FakeDiagnosticService extends DiagnosticService {
        private final List<Map<String, Object>> wal;
        private final Map<String, Object> manifest;
        private final List<Map<String, Object>> sst;

        FakeDiagnosticService(SimpleKvStorageProperties properties) {
            super(properties);
            Map<String, Object> walRow = new LinkedHashMap<String, Object>();
            walRow.put("key", "k");
            walRow.put("sequence", 1L);
            this.wal = Collections.singletonList(walRow);

            this.manifest = new LinkedHashMap<String, Object>();
            manifest.put("fileCount", 1);
            manifest.put("files", Collections.singletonList(Collections.singletonMap("fileName", "sst-1-L0.sst")));

            Map<String, Object> sstRow = new LinkedHashMap<String, Object>();
            sstRow.put("fileName", "sst-1-L0.sst");
            sstRow.put("exists", true);
            this.sst = Collections.singletonList(sstRow);
        }

        @Override
        public List<Map<String, Object>> walTail(int limit) {
            return wal;
        }

        @Override
        public Map<String, Object> manifestInspect() {
            return manifest;
        }

        @Override
        public List<Map<String, Object>> sstInspect() {
            return sst;
        }
    }
}
