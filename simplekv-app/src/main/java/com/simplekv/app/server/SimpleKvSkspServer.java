package com.simplekv.app.server;

import com.simplekv.api.model.EngineStats;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.app.cli.SimpleKvCli;
import com.simplekv.app.cli.SimpleKvCommandLineParser;
import com.simplekv.app.config.SimpleKvServerProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class SimpleKvSkspServer {
    private static final Logger log = LoggerFactory.getLogger(SimpleKvSkspServer.class);
    private static final int DEFAULT_KEYS_LIMIT = 1000;
    private static final String LIST_PREFIX = "__sksp:list__:";
    private static final String SET_PREFIX = "__sksp:set__:";
    private static final String HASH_PREFIX = "__sksp:hash__:";
    private static final String WRONGTYPE_MESSAGE =
            "WRONGTYPE Operation against a key holding the wrong kind of value";

    private final SimpleKvCli cli;
    private final KeyValueStore store;
    private final SimpleKvServerBanner banner;
    private final String host;
    private final int port;
    private final int backlog;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch started = new CountDownLatch(1);

    private volatile ServerSocket serverSocket;
    private volatile int boundPort;

    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "Server retains shared collaborators (KeyValueStore, SimpleKvServerProperties) managed by application lifecycle."
    )
    public SimpleKvSkspServer(SimpleKvCli cli,
                              KeyValueStore store,
                              SimpleKvServerProperties properties,
                              SimpleKvServerBanner banner) {
        this.cli = cli;
        this.store = store;
        this.banner = banner;
        this.host = properties.getHost();
        this.port = properties.getPort();
        this.backlog = properties.getBacklog();
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        try (ServerSocket listener = new ServerSocket()) {
            serverSocket = listener;
            listener.setReuseAddress(true);
            listener.bind(new InetSocketAddress(host, port), backlog);
            boundPort = listener.getLocalPort();
            started.countDown();

            banner.print(boundPort);
            log.info("Ready to accept connections on {}:{}", host, Integer.valueOf(boundPort));

            while (running.get()) {
                try {
                    Socket client = listener.accept();
                    Thread.startVirtualThread(() -> handleClient(client));
                }
                catch (SocketException ex) {
                    if (running.get()) {
                        throw ex;
                    }
                    break;
                }
            }
        }
        finally {
            running.set(false);
            serverSocket = null;
            started.countDown();
        }
    }

    public boolean awaitStarted(long timeout, TimeUnit unit) throws InterruptedException {
        return started.await(timeout, unit);
    }

    public int getBoundPort() {
        return boundPort;
    }

    public void close() throws IOException {
        running.set(false);
        ServerSocket listener = serverSocket;
        if (listener != null && !listener.isClosed()) {
            listener.close();
        }
    }

    private void handleClient(Socket client) {
        try (Socket socket = client;
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    writeFrame(writer, "ERR", 2, listOf("empty command"));
                    continue;
                }

                List<String> tokens;
                try {
                    tokens = SimpleKvCommandLineParser.tokenize(trimmed);
                }
                catch (IllegalArgumentException ex) {
                    writeFrame(writer, "ERR", 2, listOf("invalid command: " + ex.getMessage()));
                    continue;
                }
                if (tokens.isEmpty()) {
                    writeFrame(writer, "ERR", 2, listOf("empty command"));
                    continue;
                }

                ServerCommandResult redisLikeResult = tryHandleRedisLikeCommand(tokens);
                if (redisLikeResult != null) {
                    writeFrame(writer, redisLikeResult.status, redisLikeResult.exitCode, redisLikeResult.payload);
                    if (redisLikeResult.closeConnection) {
                        break;
                    }
                    continue;
                }

                if (isInteractive(tokens.get(0))) {
                    writeFrame(writer, "ERR", 2, listOf("interactive mode is not supported over SKSP"));
                    continue;
                }

                ByteArrayOutputStream capture = new ByteArrayOutputStream();
                int exitCode = cli.run(tokens.toArray(new String[0]), new ByteArrayInputStream(new byte[0]),
                        new PrintStream(capture, true, StandardCharsets.UTF_8));
                String payload = new String(capture.toByteArray(), StandardCharsets.UTF_8);
                writeFrame(writer, exitCode == 0 ? "OK" : "ERR", exitCode, splitLines(payload));
            }
        }
        catch (IOException ex) {
            log.debug("SKSP client disconnected: {}", ex.getMessage());
        }
    }

    private ServerCommandResult tryHandleRedisLikeCommand(List<String> tokens) throws IOException {
        String command = tokens.get(0).toUpperCase(Locale.ROOT);

        if ("PING".equals(command)) {
            String pong = tokens.size() > 1 ? joinFrom(tokens, 1) : "PONG";
            return ok(listOf(pong));
        }
        if ("QUIT".equals(command) || "EXIT".equals(command)) {
            return okAndClose(listOf("bye"));
        }
        if ("ECHO".equals(command)) {
            requireAtLeast(tokens, 2, "ECHO <message>");
            return ok(listOf(joinFrom(tokens, 1)));
        }
        if ("SET".equals(command)) {
            requireAtLeast(tokens, 3, "SET <key> <value>");
            store.put(tokens.get(1), joinFrom(tokens, 2).getBytes(StandardCharsets.UTF_8));
            return ok(listOf("OK"));
        }
        if ("GET".equals(command)) {
            requireExactly(tokens, 2, "GET <key>");
            Optional<byte[]> value = store.get(tokens.get(1));
            if (value.isPresent() && isStructuredValue(value.get())) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            return ok(listOf(value.map(SimpleKvSkspServer::decode).orElse("(nil)")));
        }
        if ("DEL".equals(command)) {
            requireAtLeast(tokens, 2, "DEL <key> [key ...]");
            long deleted = 0L;
            for (int i = 1; i < tokens.size(); i++) {
                Optional<byte[]> value = store.get(tokens.get(i));
                if (value.isPresent()) {
                    store.delete(tokens.get(i));
                    deleted++;
                }
            }
            return ok(listOf(Long.toString(deleted)));
        }
        if ("EXISTS".equals(command)) {
            requireAtLeast(tokens, 2, "EXISTS <key> [key ...]");
            long exists = 0L;
            for (int i = 1; i < tokens.size(); i++) {
                if (store.get(tokens.get(i)).isPresent()) {
                    exists++;
                }
            }
            return ok(listOf(Long.toString(exists)));
        }
        if ("INCR".equals(command) || "DECR".equals(command)) {
            requireExactly(tokens, 2, command + " <key>");
            long current = parseLongValue(store.get(tokens.get(1)).map(SimpleKvSkspServer::decode).orElse("0"));
            long next = "INCR".equals(command) ? current + 1L : current - 1L;
            store.put(tokens.get(1), Long.toString(next).getBytes(StandardCharsets.UTF_8));
            return ok(listOf(Long.toString(next)));
        }
        if ("MSET".equals(command)) {
            if (tokens.size() < 3 || tokens.size() % 2 == 0) {
                return err(2, "usage: MSET <key> <value> [key value ...]");
            }
            for (int i = 1; i < tokens.size(); i += 2) {
                store.put(tokens.get(i), tokens.get(i + 1).getBytes(StandardCharsets.UTF_8));
            }
            return ok(listOf("OK"));
        }
        if ("MGET".equals(command)) {
            requireAtLeast(tokens, 2, "MGET <key> [key ...]");
            List<String> values = new ArrayList<String>();
            for (int i = 1; i < tokens.size(); i++) {
                Optional<byte[]> value = store.get(tokens.get(i));
                if (value.isPresent() && isStructuredValue(value.get())) {
                    values.add("(error) " + WRONGTYPE_MESSAGE);
                }
                else {
                    values.add(value.map(SimpleKvSkspServer::decode).orElse("(nil)"));
                }
            }
            return ok(values);
        }
        if ("LPUSH".equals(command)) {
            requireAtLeast(tokens, 3, "LPUSH <key> <value> [value ...]");
            String key = tokens.get(1);
            List<String> list = loadList(key);
            if (list == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            for (int i = 2; i < tokens.size(); i++) {
                list.add(0, tokens.get(i));
            }
            saveList(key, list);
            return ok(listOf(Integer.toString(list.size())));
        }
        if ("RPUSH".equals(command)) {
            requireAtLeast(tokens, 3, "RPUSH <key> <value> [value ...]");
            String key = tokens.get(1);
            List<String> list = loadList(key);
            if (list == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            for (int i = 2; i < tokens.size(); i++) {
                list.add(tokens.get(i));
            }
            saveList(key, list);
            return ok(listOf(Integer.toString(list.size())));
        }
        if ("LPOP".equals(command)) {
            requireExactly(tokens, 2, "LPOP <key>");
            String key = tokens.get(1);
            List<String> list = loadList(key);
            if (list == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            if (list.isEmpty()) {
                return ok(listOf("(nil)"));
            }
            String value = list.remove(0);
            saveList(key, list);
            return ok(listOf(value));
        }
        if ("RPOP".equals(command)) {
            requireExactly(tokens, 2, "RPOP <key>");
            String key = tokens.get(1);
            List<String> list = loadList(key);
            if (list == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            if (list.isEmpty()) {
                return ok(listOf("(nil)"));
            }
            String value = list.remove(list.size() - 1);
            saveList(key, list);
            return ok(listOf(value));
        }
        if ("LLEN".equals(command)) {
            requireExactly(tokens, 2, "LLEN <key>");
            List<String> list = loadList(tokens.get(1));
            if (list == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            return ok(listOf(Integer.toString(list.size())));
        }
        if ("LRANGE".equals(command)) {
            requireExactly(tokens, 4, "LRANGE <key> <start> <stop>");
            List<String> list = loadList(tokens.get(1));
            if (list == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            int start = Integer.parseInt(tokens.get(2));
            int stop = Integer.parseInt(tokens.get(3));
            return ok(sliceList(list, start, stop));
        }
        if ("SADD".equals(command)) {
            requireAtLeast(tokens, 3, "SADD <key> <member> [member ...]");
            String key = tokens.get(1);
            Set<String> set = loadSet(key);
            if (set == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            int added = 0;
            for (int i = 2; i < tokens.size(); i++) {
                if (set.add(tokens.get(i))) {
                    added++;
                }
            }
            saveSet(key, set);
            return ok(listOf(Integer.toString(added)));
        }
        if ("SREM".equals(command)) {
            requireAtLeast(tokens, 3, "SREM <key> <member> [member ...]");
            String key = tokens.get(1);
            Set<String> set = loadSet(key);
            if (set == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            int removed = 0;
            for (int i = 2; i < tokens.size(); i++) {
                if (set.remove(tokens.get(i))) {
                    removed++;
                }
            }
            saveSet(key, set);
            return ok(listOf(Integer.toString(removed)));
        }
        if ("SMEMBERS".equals(command)) {
            requireExactly(tokens, 2, "SMEMBERS <key>");
            Set<String> set = loadSet(tokens.get(1));
            if (set == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            List<String> members = new ArrayList<String>(set);
            Collections.sort(members);
            return ok(members);
        }
        if ("SISMEMBER".equals(command)) {
            requireExactly(tokens, 3, "SISMEMBER <key> <member>");
            Set<String> set = loadSet(tokens.get(1));
            if (set == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            return ok(listOf(set.contains(tokens.get(2)) ? "1" : "0"));
        }
        if ("SCARD".equals(command)) {
            requireExactly(tokens, 2, "SCARD <key>");
            Set<String> set = loadSet(tokens.get(1));
            if (set == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            return ok(listOf(Integer.toString(set.size())));
        }
        if ("HSET".equals(command)) {
            if (tokens.size() < 4 || tokens.size() % 2 != 0) {
                return err(2, "usage: HSET <key> <field> <value> [field value ...]");
            }
            String key = tokens.get(1);
            Map<String, String> hash = loadHash(key);
            if (hash == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            int created = 0;
            for (int i = 2; i < tokens.size(); i += 2) {
                String field = tokens.get(i);
                String value = tokens.get(i + 1);
                if (!hash.containsKey(field)) {
                    created++;
                }
                hash.put(field, value);
            }
            saveHash(key, hash);
            return ok(listOf(Integer.toString(created)));
        }
        if ("HGET".equals(command)) {
            requireExactly(tokens, 3, "HGET <key> <field>");
            Map<String, String> hash = loadHash(tokens.get(1));
            if (hash == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            return ok(listOf(hash.getOrDefault(tokens.get(2), "(nil)")));
        }
        if ("HDEL".equals(command)) {
            requireAtLeast(tokens, 3, "HDEL <key> <field> [field ...]");
            String key = tokens.get(1);
            Map<String, String> hash = loadHash(key);
            if (hash == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            int removed = 0;
            for (int i = 2; i < tokens.size(); i++) {
                if (hash.remove(tokens.get(i)) != null) {
                    removed++;
                }
            }
            saveHash(key, hash);
            return ok(listOf(Integer.toString(removed)));
        }
        if ("HEXISTS".equals(command)) {
            requireExactly(tokens, 3, "HEXISTS <key> <field>");
            Map<String, String> hash = loadHash(tokens.get(1));
            if (hash == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            return ok(listOf(hash.containsKey(tokens.get(2)) ? "1" : "0"));
        }
        if ("HLEN".equals(command)) {
            requireExactly(tokens, 2, "HLEN <key>");
            Map<String, String> hash = loadHash(tokens.get(1));
            if (hash == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            return ok(listOf(Integer.toString(hash.size())));
        }
        if ("HGETALL".equals(command)) {
            requireExactly(tokens, 2, "HGETALL <key>");
            Map<String, String> hash = loadHash(tokens.get(1));
            if (hash == null) {
                return err(2, WRONGTYPE_MESSAGE);
            }
            List<String> lines = new ArrayList<String>();
            for (Map.Entry<String, String> entry : hash.entrySet()) {
                lines.add(entry.getKey());
                lines.add(entry.getValue());
            }
            return ok(lines);
        }
        if ("INFO".equals(command)) {
            EngineStats stats = store.stats();
            List<String> lines = new ArrayList<String>();
            lines.add("server:SimpleKV");
            lines.add("protocol:SKSP/1.0");
            lines.add("writes:" + stats.getWrites());
            lines.add("reads:" + stats.getReads());
            lines.add("deletes:" + stats.getDeletes());
            lines.add("scans:" + stats.getScans());
            lines.add("l0Files:" + stats.getL0Files());
            lines.add("l1Files:" + stats.getL1Files());
            return ok(lines);
        }
        if ("KEYS".equals(command)) {
            requireExactly(tokens, 2, "KEYS <pattern>");
            String pattern = tokens.get(1);
            List<KeyValue> values;
            if ("*".equals(pattern)) {
                values = store.scan("", "\uffff", DEFAULT_KEYS_LIMIT);
            }
            else if (pattern.endsWith("*")) {
                values = store.prefixScan(pattern.substring(0, pattern.length() - 1), DEFAULT_KEYS_LIMIT);
            }
            else {
                values = store.get(pattern).isPresent()
                        ? List.of(new KeyValue(pattern, null))
                        : List.of();
            }
            List<String> keys = new ArrayList<String>();
            for (KeyValue value : values) {
                keys.add(value.getKey());
            }
            return ok(keys);
        }
        if ("COMMAND".equals(command) || "HELP".equals(command)) {
            return ok(List.of(
                    "PING [message]",
                    "QUIT",
                    "ECHO <message>",
                    "SET <key> <value>",
                    "GET <key>",
                    "DEL <key> [key ...]",
                    "EXISTS <key> [key ...]",
                    "INCR <key>",
                    "DECR <key>",
                    "MSET <key> <value> [key value ...]",
                    "MGET <key> [key ...]",
                    "LPUSH <key> <value> [value ...]",
                    "RPUSH <key> <value> [value ...]",
                    "LPOP <key>",
                    "RPOP <key>",
                    "LLEN <key>",
                    "LRANGE <key> <start> <stop>",
                    "SADD <key> <member> [member ...]",
                    "SREM <key> <member> [member ...]",
                    "SMEMBERS <key>",
                    "SISMEMBER <key> <member>",
                    "SCARD <key>",
                    "HSET <key> <field> <value> [field value ...]",
                    "HGET <key> <field>",
                    "HDEL <key> <field> [field ...]",
                    "HEXISTS <key> <field>",
                    "HLEN <key>",
                    "HGETALL <key>",
                    "KEYS <pattern>",
                    "INFO",
                    "COMMAND",
                    "HELP"
            ));
        }
        return null;
    }

    private static void writeFrame(BufferedWriter writer, String status, int exitCode, List<String> payload)
            throws IOException {
        writer.write("SKSP/1.0 ");
        writer.write(status);
        writer.write(' ');
        writer.write(Integer.toString(exitCode));
        writer.write("\r\n");
        writer.write(Integer.toString(payload.size()));
        writer.write("\r\n");
        for (String line : payload) {
            writer.write(line);
            writer.write("\r\n");
        }
        writer.flush();
    }

    private static List<String> splitLines(String payload) {
        if (payload.isEmpty()) {
            return List.of();
        }

        List<String> lines = new ArrayList<String>(Arrays.asList(payload.split("\\R", -1)));
        if (!lines.isEmpty() && lines.get(lines.size() - 1).isEmpty()) {
            lines.remove(lines.size() - 1);
        }
        return lines;
    }

    private static List<String> listOf(String value) {
        return List.of(value);
    }

    private static ServerCommandResult ok(List<String> payload) {
        return new ServerCommandResult("OK", 0, payload, false);
    }

    private static ServerCommandResult okAndClose(List<String> payload) {
        return new ServerCommandResult("OK", 0, payload, true);
    }

    private static ServerCommandResult err(int exitCode, String message) {
        return new ServerCommandResult("ERR", exitCode, listOf(message), false);
    }

    private static void requireExactly(List<String> tokens, int expected, String usage) {
        if (tokens.size() != expected) {
            throw new IllegalArgumentException("usage: " + usage);
        }
    }

    private static void requireAtLeast(List<String> tokens, int expected, String usage) {
        if (tokens.size() < expected) {
            throw new IllegalArgumentException("usage: " + usage);
        }
    }

    private static long parseLongValue(String value) {
        try {
            return Long.parseLong(value);
        }
        catch (NumberFormatException ex) {
            throw new IllegalArgumentException("value is not an integer or out of range");
        }
    }

    private static String joinFrom(List<String> tokens, int start) {
        StringBuilder builder = new StringBuilder();
        for (int i = start; i < tokens.size(); i++) {
            if (i > start) {
                builder.append(' ');
            }
            builder.append(tokens.get(i));
        }
        return builder.toString();
    }

    private static String decode(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    private List<String> loadList(String key) throws IOException {
        Optional<byte[]> raw = store.get(key);
        if (raw.isEmpty()) {
            return new ArrayList<String>();
        }
        String text = decode(raw.get());
        if (text.startsWith(LIST_PREFIX)) {
            return decodeList(text.substring(LIST_PREFIX.length()));
        }
        return null;
    }

    private void saveList(String key, List<String> values) throws IOException {
        store.put(key, (LIST_PREFIX + encodeList(values)).getBytes(StandardCharsets.UTF_8));
    }

    private Set<String> loadSet(String key) throws IOException {
        Optional<byte[]> raw = store.get(key);
        if (raw.isEmpty()) {
            return new LinkedHashSet<String>();
        }
        String text = decode(raw.get());
        if (text.startsWith(SET_PREFIX)) {
            return decodeSet(text.substring(SET_PREFIX.length()));
        }
        return null;
    }

    private void saveSet(String key, Set<String> members) throws IOException {
        store.put(key, (SET_PREFIX + encodeSet(members)).getBytes(StandardCharsets.UTF_8));
    }

    private Map<String, String> loadHash(String key) throws IOException {
        Optional<byte[]> raw = store.get(key);
        if (raw.isEmpty()) {
            return new LinkedHashMap<String, String>();
        }
        String text = decode(raw.get());
        if (text.startsWith(HASH_PREFIX)) {
            return decodeHash(text.substring(HASH_PREFIX.length()));
        }
        return null;
    }

    private void saveHash(String key, Map<String, String> hash) throws IOException {
        store.put(key, (HASH_PREFIX + encodeHash(hash)).getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> sliceList(List<String> list, int start, int stop) {
        int size = list.size();
        if (size == 0) {
            return List.of();
        }
        int from = start >= 0 ? start : size + start;
        int to = stop >= 0 ? stop : size + stop;

        if (from < 0) {
            from = 0;
        }
        if (to < 0) {
            return List.of();
        }
        if (from >= size) {
            return List.of();
        }
        if (to >= size) {
            to = size - 1;
        }
        if (from > to) {
            return List.of();
        }
        return new ArrayList<String>(list.subList(from, to + 1));
    }

    private static boolean isStructuredValue(byte[] rawValue) {
        String text = decode(rawValue);
        return text.startsWith(LIST_PREFIX) || text.startsWith(SET_PREFIX) || text.startsWith(HASH_PREFIX);
    }

    private static String encodeList(List<String> values) {
        return encodeTokens(values);
    }

    private static List<String> decodeList(String payload) {
        return new ArrayList<String>(decodeTokens(payload));
    }

    private static String encodeSet(Set<String> members) {
        return encodeTokens(new ArrayList<String>(members));
    }

    private static Set<String> decodeSet(String payload) {
        return new LinkedHashSet<String>(decodeTokens(payload));
    }

    private static String encodeHash(Map<String, String> hash) {
        List<String> pairs = new ArrayList<String>();
        for (Map.Entry<String, String> entry : hash.entrySet()) {
            pairs.add(entry.getKey() + "\u0000" + entry.getValue());
        }
        return encodeTokens(pairs);
    }

    private static Map<String, String> decodeHash(String payload) {
        List<String> pairs = decodeTokens(payload);
        Map<String, String> hash = new LinkedHashMap<String, String>();
        for (String pair : pairs) {
            int splitIndex = pair.indexOf('\u0000');
            if (splitIndex <= 0) {
                continue;
            }
            hash.put(pair.substring(0, splitIndex), pair.substring(splitIndex + 1));
        }
        return hash;
    }

    private static String encodeTokens(List<String> tokens) {
        if (tokens.isEmpty()) {
            return "";
        }
        List<String> encoded = new ArrayList<String>();
        for (String token : tokens) {
            encoded.add(Base64.getEncoder().encodeToString(token.getBytes(StandardCharsets.UTF_8)));
        }
        return String.join(",", encoded);
    }

    private static List<String> decodeTokens(String payload) {
        if (payload == null || payload.isEmpty()) {
            return List.of();
        }
        String[] encoded = payload.split(",", -1);
        List<String> decoded = new ArrayList<String>();
        for (String token : encoded) {
            byte[] value = Base64.getDecoder().decode(token);
            decoded.add(new String(value, StandardCharsets.UTF_8));
        }
        return decoded;
    }

    private static boolean isInteractive(String command) {
        return "repl".equalsIgnoreCase(command)
                || "connect".equalsIgnoreCase(command)
                || "shell".equalsIgnoreCase(command);
    }

    private static final class ServerCommandResult {
        private final String status;
        private final int exitCode;
        private final List<String> payload;
        private final boolean closeConnection;

        private ServerCommandResult(String status, int exitCode, List<String> payload, boolean closeConnection) {
            this.status = status;
            this.exitCode = exitCode;
            this.payload = payload;
            this.closeConnection = closeConnection;
        }
    }
}
