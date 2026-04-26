package com.simplekv.app.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekv.api.model.EngineStats;
import com.simplekv.api.model.KeyValue;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.app.diag.DiagnosticService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.BufferedReader;
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

@Component
public class SimpleKvCli {
    private static final int DEFAULT_TAIL_LIMIT = 20;
    private static final String REPL_COMMAND = "repl";
    private static final String LIST_PREFIX = "__sksp:list__:";
    private static final String SET_PREFIX = "__sksp:set__:";
    private static final String HASH_PREFIX = "__sksp:hash__:";
    private static final String WRONGTYPE_MESSAGE =
            "WRONGTYPE Operation against a key holding the wrong kind of value";
    
    
    private static final Map<String, String> COMMAND_DESCRIPTIONS = new LinkedHashMap<>();
    
    static {
        
        COMMAND_DESCRIPTIONS.put("put", "Set a key-value pair. Usage: put <key> <value>");
        COMMAND_DESCRIPTIONS.put("ttl-put", "Set a key-value pair with expiration. Usage: ttl-put <key> <value> <expireAtMillis>");
        COMMAND_DESCRIPTIONS.put("get", "Get the value of a key. Usage: get <key>");
        COMMAND_DESCRIPTIONS.put("delete", "Delete a key. Usage: delete <key>");
        COMMAND_DESCRIPTIONS.put("scan", "Scan keys in range. Usage: scan <startKey> <endKey> [limit]");
        COMMAND_DESCRIPTIONS.put("prefix", "Scan keys with prefix. Usage: prefix <prefix> [limit]");
        
        
        COMMAND_DESCRIPTIONS.put("lpush", "Push values to the left of a list. Usage: lpush <key> <value> [value ...]");
        COMMAND_DESCRIPTIONS.put("rpush", "Push values to the right of a list. Usage: rpush <key> <value> [value ...]");
        COMMAND_DESCRIPTIONS.put("lpop", "Pop from the left of a list. Usage: lpop <key>");
        COMMAND_DESCRIPTIONS.put("rpop", "Pop from the right of a list. Usage: rpop <key>");
        COMMAND_DESCRIPTIONS.put("llen", "Get the length of a list. Usage: llen <key>");
        COMMAND_DESCRIPTIONS.put("lrange", "Get a range from a list. Usage: lrange <key> <start> <stop>");
        
        COMMAND_DESCRIPTIONS.put("sadd", "Add members to a set. Usage: sadd <key> <member> [member ...]");
        COMMAND_DESCRIPTIONS.put("srem", "Remove members from a set. Usage: srem <key> <member> [member ...]");
        COMMAND_DESCRIPTIONS.put("smembers", "Get all members of a set. Usage: smembers <key>");
        COMMAND_DESCRIPTIONS.put("sismember", "Check if a member is in a set. Usage: sismember <key> <member>");
        COMMAND_DESCRIPTIONS.put("scard", "Get the cardinality of a set. Usage: scard <key>");
        
        COMMAND_DESCRIPTIONS.put("hset", "Set hash fields. Usage: hset <key> <field> <value> [field value ...]");
        COMMAND_DESCRIPTIONS.put("hget", "Get a hash field. Usage: hget <key> <field>");
        COMMAND_DESCRIPTIONS.put("hdel", "Delete hash fields. Usage: hdel <key> <field> [field ...]");
        COMMAND_DESCRIPTIONS.put("hexists", "Check if hash field exists. Usage: hexists <key> <field>");
        COMMAND_DESCRIPTIONS.put("hlen", "Get the number of fields in a hash. Usage: hlen <key>");
        COMMAND_DESCRIPTIONS.put("hgetall", "Get all fields and values of a hash. Usage: hgetall <key>");
        
        
        COMMAND_DESCRIPTIONS.put("stats", "Get engine statistics. Usage: stats");
        COMMAND_DESCRIPTIONS.put("sst-dump", "Dump all SST files. Usage: sst-dump");
        COMMAND_DESCRIPTIONS.put("recover", "Recover from data directory. Usage: recover");
        COMMAND_DESCRIPTIONS.put("compact", "Trigger a flush/compaction. Usage: compact");
        COMMAND_DESCRIPTIONS.put("trace-key", "Trace a key in manifests. Usage: trace-key <key>");
        COMMAND_DESCRIPTIONS.put("wal-tail", "Show WAL tail entries. Usage: wal-tail [limit]");
        COMMAND_DESCRIPTIONS.put("sst-inspect", "Inspect all SST files. Usage: sst-inspect");
        COMMAND_DESCRIPTIONS.put("manifest-inspect", "Inspect manifest files. Usage: manifest-inspect");
    }

    private final KeyValueStore store;
    private final DiagnosticService diagnosticService;
    private final ObjectMapper objectMapper;
    private final SimpleKvStorageProperties properties;

    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP2",
            justification = "Spring-managed collaborators are intentionally retained for CLI orchestration."
    )
    public SimpleKvCli(KeyValueStore store,
                       DiagnosticService diagnosticService,
                       ObjectMapper objectMapper,
                       SimpleKvStorageProperties properties) {
        this.store = store;
        this.diagnosticService = diagnosticService;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public int run(String[] args, PrintStream out) {
        return run(args, System.in, out);
    }

    public int run(String[] args, InputStream in, PrintStream out) {
        if (args == null || args.length == 0) {
            printHelp(out);
            return 0;
        }

        boolean json = false;
        int cursor = 0;
        if ("--json".equalsIgnoreCase(args[cursor])) {
            json = true;
            cursor++;
        }
        if (cursor >= args.length) {
            printHelp(out);
            return 0;
        }

        String command = args[cursor].toLowerCase(Locale.ROOT);
        if (REPL_COMMAND.equals(command) || "connect".equals(command) || "shell".equals(command)) {
            return runRepl(in, out, json);
        }
        
        
        if ("help".equals(command)) {
            if (cursor + 1 < args.length) {
                
                String topic = args[cursor + 1];
                printDetailedHelp(topic, out);
            } else {
                
                printHelp(out);
            }
            return 0;
        }
        
        String[] commandArgs = Arrays.copyOfRange(args, cursor + 1, args.length);
        return runSingleCommand(command, commandArgs, json, out, true);
    }

    private int runRepl(InputStream in, PrintStream out, boolean defaultJson) {
        out.println("Connected to SimpleKV interactive shell.");
        out.println("Type 'help' for commands, 'quit' to exit.");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        try {
            while (true) {
                out.print("simplekv> ");
                out.flush();
                String line = reader.readLine();
                if (line == null) {
                    return 0;
                }
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if ("quit".equalsIgnoreCase(trimmed) || "exit".equalsIgnoreCase(trimmed)) {
                    out.println("bye");
                    return 0;
                }
                if ("help".equalsIgnoreCase(trimmed) || "?".equals(trimmed)) {
                    printHelp(out);
                    continue;
                }
                
                
                if (trimmed.toLowerCase(Locale.ROOT).startsWith("help ")) {
                    String topic = trimmed.substring(5).trim();
                    if (!topic.isEmpty()) {
                        printDetailedHelp(topic, out);
                        continue;
                    }
                }
                
                
                String lowerTrimmed = trimmed.toLowerCase(Locale.ROOT);
                if (lowerTrimmed.startsWith("simplekv help ") || lowerTrimmed.startsWith("kv help ")) {
                    int helpIndex = trimmed.indexOf("help");
                    String topic = trimmed.substring(helpIndex + 4).trim();
                    if (!topic.isEmpty()) {
                        printDetailedHelp(topic, out);
                        continue;
                    }
                }

                List<String> tokens = SimpleKvCommandLineParser.tokenize(trimmed);
                boolean json = defaultJson;
                if (!tokens.isEmpty() && "--json".equalsIgnoreCase(tokens.get(0))) {
                    json = true;
                    tokens.remove(0);
                }
                
                
                if (!tokens.isEmpty()) {
                    String firstToken = tokens.get(0).toLowerCase(Locale.ROOT);
                    if ("simplekv".equals(firstToken) || "kv".equals(firstToken)) {
                        tokens.remove(0);
                    }
                }
                
                if (tokens.isEmpty()) {
                    continue;
                }
                String command = tokens.get(0).toLowerCase(Locale.ROOT);
                String[] commandArgs = tokens.subList(1, tokens.size()).toArray(new String[0]);
                runSingleCommand(command, commandArgs, json, out, false);
            }
        }
        catch (IllegalArgumentException ex) {
            out.println("invalid command: " + ex.getMessage());
            return 2;
        }
        catch (IOException ex) {
            out.println("io error: " + ex.getMessage());
            return 1;
        }
    }

    private int runSingleCommand(String command,
                                 String[] commandArgs,
                                 boolean json,
                                 PrintStream out,
                                 boolean printHelpOnInvalid) {
        try {
            Object result = execute(command, commandArgs);
            printResult(result, json, out);
            return 0;
        }
        catch (IllegalArgumentException ex) {
            out.println("invalid command: " + ex.getMessage());
            if (printHelpOnInvalid) {
                printHelp(out);
            }
            return 2;
        }
        catch (IOException ex) {
            out.println("io error: " + ex.getMessage());
            return 1;
        }
    }

    public void printHelp(PrintStream out) {
        out.println("SimpleKV CLI");
        out.println("usage: simplekv [--json] <command> [args]");
        out.println("       simplekv [--json] repl");
        out.println();
        out.println("The commands are:");
        out.println();
        
        
        out.println("Key-Value Operations:");
        out.println("  put               Set a key-value pair");
        out.println("  ttl-put           Set a key-value pair with expiration");
        out.println("  get               Get the value of a key");
        out.println("  delete            Delete a key");
        out.println("  scan              Scan keys in range");
        out.println("  prefix            Scan keys with prefix");
        out.println();
        
        
        out.println("Collection Operations:");
        out.println("  lpush             Push values to the left of a list");
        out.println("  rpush             Push values to the right of a list");
        out.println("  lpop              Pop from the left of a list");
        out.println("  rpop              Pop from the right of a list");
        out.println("  llen              Get the length of a list");
        out.println("  lrange            Get a range from a list");
        out.println("  sadd              Add members to a set");
        out.println("  srem              Remove members from a set");
        out.println("  smembers          Get all members of a set");
        out.println("  sismember         Check if a member is in a set");
        out.println("  scard             Get the cardinality of a set");
        out.println("  hset              Set hash fields");
        out.println("  hget              Get a hash field");
        out.println("  hdel              Delete hash fields");
        out.println("  hexists           Check if hash field exists");
        out.println("  hlen              Get the number of fields in a hash");
        out.println("  hgetall           Get all fields and values of a hash");
        out.println();
        
        
        out.println("Diagnostic Commands:");
        out.println("  stats             Get engine statistics");
        out.println("  sst-dump          Dump all SST files");
        out.println("  recover           Recover from data directory");
        out.println("  compact           Trigger a flush/compaction");
        out.println("  trace-key         Trace a key in manifests");
        out.println("  wal-tail          Show WAL tail entries");
        out.println("  sst-inspect       Inspect all SST files");
        out.println("  manifest-inspect  Inspect manifest files");
        out.println();
        
        
        out.println("Interactive Commands:");
        out.println("  repl              Start interactive shell (or 'connect', 'shell')");
        out.println("  help              Show this help message or details for a command");
        out.println("  quit              Exit the interactive shell");
        out.println();
        
        out.println("Use \"simplekv help <command>\" for more information about a command.");
    }
    
    private void printDetailedHelp(String topic, PrintStream out) {
        String description = COMMAND_DESCRIPTIONS.get(topic.toLowerCase(Locale.ROOT));
        if (description != null) {
            out.println("Command: " + topic);
            out.println(description);
        } else {
            out.println("Unknown command or topic: " + topic);
            out.println("Use 'simplekv help' for a list of commands.");
        }
    }

    private Object execute(String command, String[] args) throws IOException {
        if ("put".equals(command)) {
            requireArgs(command, args, 2);
            store.put(args[0], args[1].getBytes(StandardCharsets.UTF_8));
            return ok("put", args[0]);
        }
        if ("ttl-put".equals(command)) {
            requireArgs(command, args, 3);
            store.ttlPut(args[0], args[1].getBytes(StandardCharsets.UTF_8), parseLong(args[2], "expireAtMillis"));
            return ok("ttl-put", args[0]);
        }
        if ("get".equals(command)) {
            requireArgs(command, args, 1);
            Optional<byte[]> value = store.get(args[0]);
            if (value.isPresent() && isStructuredValue(value.get())) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("key", args[0]);
            result.put("found", Boolean.valueOf(value.isPresent()));
            result.put("value", value.isPresent() ? decode(value.get()) : null);
            return result;
        }
        if ("delete".equals(command)) {
            requireArgs(command, args, 1);
            store.delete(args[0]);
            return ok("delete", args[0]);
        }
        if ("scan".equals(command)) {
            requireArgs(command, args, 2);
            int limit = args.length >= 3 ? parseInt(args[2], "limit") : properties.getScanDefaultLimit();
            return kvList(store.scan(args[0], args[1], limit));
        }
        if ("prefix".equals(command)) {
            requireArgs(command, args, 1);
            int limit = args.length >= 2 ? parseInt(args[1], "limit") : properties.getScanDefaultLimit();
            return kvList(store.prefixScan(args[0], limit));
        }
        if ("stats".equals(command)) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("summary", store.statsCommand());
            result.put("stats", statsMap(store.stats()));
            return result;
        }
        if ("sst-dump".equals(command)) {
            return store.sstDump();
        }
        if ("recover".equals(command)) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("dataDir", diagnosticService.dataDir().toString());
            result.put("stats", statsMap(store.stats()));
            result.put("manifest", diagnosticService.manifestInspect());
            return result;
        }
        if ("compact".equals(command)) {
            store.flush();
            return ok("compact", "flush-triggered");
        }
        if ("trace-key".equals(command)) {
            requireArgs(command, args, 1);
            return traceKey(args[0]);
        }
        if ("wal-tail".equals(command)) {
            int limit = args.length >= 1 ? parseInt(args[0], "limit") : DEFAULT_TAIL_LIMIT;
            return diagnosticService.walTail(limit);
        }
        if ("sst-inspect".equals(command)) {
            return diagnosticService.sstInspect();
        }
        if ("manifest-inspect".equals(command)) {
            return diagnosticService.manifestInspect();
        }
        if ("lpush".equals(command)) {
            requireArgs(command, args, 2);
            List<String> list = loadList(args[0]);
            if (list == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            for (int i = 1; i < args.length; i++) {
                list.add(0, args[i]);
            }
            saveList(args[0], list);
            return Integer.valueOf(list.size());
        }
        if ("rpush".equals(command)) {
            requireArgs(command, args, 2);
            List<String> list = loadList(args[0]);
            if (list == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            for (int i = 1; i < args.length; i++) {
                list.add(args[i]);
            }
            saveList(args[0], list);
            return Integer.valueOf(list.size());
        }
        if ("lpop".equals(command)) {
            requireArgs(command, args, 1);
            List<String> list = loadList(args[0]);
            if (list == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            if (list.isEmpty()) {
                return null;
            }
            String value = list.remove(0);
            saveList(args[0], list);
            return value;
        }
        if ("rpop".equals(command)) {
            requireArgs(command, args, 1);
            List<String> list = loadList(args[0]);
            if (list == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            if (list.isEmpty()) {
                return null;
            }
            String value = list.remove(list.size() - 1);
            saveList(args[0], list);
            return value;
        }
        if ("llen".equals(command)) {
            requireArgs(command, args, 1);
            List<String> list = loadList(args[0]);
            if (list == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            return Integer.valueOf(list.size());
        }
        if ("lrange".equals(command)) {
            requireArgs(command, args, 3);
            List<String> list = loadList(args[0]);
            if (list == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            int start = parseInt(args[1], "start");
            int stop = parseInt(args[2], "stop");
            return sliceList(list, start, stop);
        }
        if ("sadd".equals(command)) {
            requireArgs(command, args, 2);
            Set<String> set = loadSet(args[0]);
            if (set == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            int added = 0;
            for (int i = 1; i < args.length; i++) {
                if (set.add(args[i])) {
                    added++;
                }
            }
            saveSet(args[0], set);
            return Integer.valueOf(added);
        }
        if ("srem".equals(command)) {
            requireArgs(command, args, 2);
            Set<String> set = loadSet(args[0]);
            if (set == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            int removed = 0;
            for (int i = 1; i < args.length; i++) {
                if (set.remove(args[i])) {
                    removed++;
                }
            }
            saveSet(args[0], set);
            return Integer.valueOf(removed);
        }
        if ("smembers".equals(command)) {
            requireArgs(command, args, 1);
            Set<String> set = loadSet(args[0]);
            if (set == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            List<String> members = new ArrayList<String>(set);
            Collections.sort(members);
            return members;
        }
        if ("sismember".equals(command)) {
            requireArgs(command, args, 2);
            Set<String> set = loadSet(args[0]);
            if (set == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            return Integer.valueOf(set.contains(args[1]) ? 1 : 0);
        }
        if ("scard".equals(command)) {
            requireArgs(command, args, 1);
            Set<String> set = loadSet(args[0]);
            if (set == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            return Integer.valueOf(set.size());
        }
        if ("hset".equals(command)) {
            if (args.length < 3 || args.length % 2 == 0) {
                throw new IllegalArgumentException("hset expects field/value pairs");
            }
            Map<String, String> hash = loadHash(args[0]);
            if (hash == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            int created = 0;
            for (int i = 1; i < args.length; i += 2) {
                if (!hash.containsKey(args[i])) {
                    created++;
                }
                hash.put(args[i], args[i + 1]);
            }
            saveHash(args[0], hash);
            return Integer.valueOf(created);
        }
        if ("hget".equals(command)) {
            requireArgs(command, args, 2);
            Map<String, String> hash = loadHash(args[0]);
            if (hash == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            return hash.get(args[1]);
        }
        if ("hdel".equals(command)) {
            requireArgs(command, args, 2);
            Map<String, String> hash = loadHash(args[0]);
            if (hash == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            int removed = 0;
            for (int i = 1; i < args.length; i++) {
                if (hash.remove(args[i]) != null) {
                    removed++;
                }
            }
            saveHash(args[0], hash);
            return Integer.valueOf(removed);
        }
        if ("hexists".equals(command)) {
            requireArgs(command, args, 2);
            Map<String, String> hash = loadHash(args[0]);
            if (hash == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            return Integer.valueOf(hash.containsKey(args[1]) ? 1 : 0);
        }
        if ("hlen".equals(command)) {
            requireArgs(command, args, 1);
            Map<String, String> hash = loadHash(args[0]);
            if (hash == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            return Integer.valueOf(hash.size());
        }
        if ("hgetall".equals(command)) {
            requireArgs(command, args, 1);
            Map<String, String> hash = loadHash(args[0]);
            if (hash == null) {
                throw new IllegalArgumentException(WRONGTYPE_MESSAGE);
            }
            return hash;
        }
        throw new IllegalArgumentException("unknown command: " + command);
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

    private void printResult(Object result, boolean json, PrintStream out) throws JsonProcessingException {
        if (json) {
            out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
            return;
        }
        if (result instanceof String) {
            out.println((String) result);
            return;
        }
        out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
    }

    private Map<String, Object> traceKey(String key) throws IOException {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        Optional<byte[]> value = store.get(key);
        result.put("key", key);
        result.put("value", value.isPresent() ? decode(value.get()) : null);

        String end = key + "~";
        List<KeyValue> nearby = store.scan(key, end, properties.getScanDefaultLimit());
        result.put("nearby", kvList(nearby));

        result.put("manifest", diagnosticService.manifestInspect());
        result.put("stats", statsMap(store.stats()));
        return result;
    }

    private static Map<String, Object> ok(String action, String key) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("status", "ok");
        result.put("action", action);
        result.put("key", key);
        return result;
    }

    private static List<Map<String, Object>> kvList(List<KeyValue> values) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (KeyValue keyValue : values) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("key", keyValue.getKey());
            row.put("value", decode(keyValue.getValue()));
            rows.add(row);
        }
        return rows;
    }

    private static Map<String, Object> statsMap(EngineStats stats) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("writes", Long.valueOf(stats.getWrites()));
        result.put("reads", Long.valueOf(stats.getReads()));
        result.put("deletes", Long.valueOf(stats.getDeletes()));
        result.put("scans", Long.valueOf(stats.getScans()));
        result.put("flushes", Long.valueOf(stats.getFlushes()));
        result.put("compactions", Long.valueOf(stats.getCompactions()));
        result.put("cacheHits", Long.valueOf(stats.getCacheHits()));
        result.put("cacheMisses", Long.valueOf(stats.getCacheMisses()));
        result.put("bloomPositives", Long.valueOf(stats.getBloomPositives()));
        result.put("bloomNegatives", Long.valueOf(stats.getBloomNegatives()));
        result.put("slowReads", Long.valueOf(stats.getSlowReads()));
        result.put("slowScans", Long.valueOf(stats.getSlowScans()));
        result.put("backpressureEvents", Long.valueOf(stats.getBackpressureEvents()));
        result.put("mutableEntries", Integer.valueOf(stats.getMutableEntries()));
        result.put("immutableEntries", Integer.valueOf(stats.getImmutableEntries()));
        result.put("l0Files", Integer.valueOf(stats.getL0Files()));
        result.put("l1Files", Integer.valueOf(stats.getL1Files()));
        result.put("writeAmplification", Double.valueOf(stats.getWriteAmplification()));
        result.put("readAmplification", Double.valueOf(stats.getReadAmplification()));
        result.put("spaceAmplification", Double.valueOf(stats.getSpaceAmplification()));
        return result;
    }

    private static int parseInt(String raw, String fieldName) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(fieldName + " must be an integer: " + raw);
        }
    }

    private static long parseLong(String raw, String fieldName) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(fieldName + " must be a long: " + raw);
        }
    }

    private static void requireArgs(String command, String[] args, int count) {
        if (args.length < count) {
            throw new IllegalArgumentException(command + " expects at least " + count + " args");
        }
    }

    private static String decode(byte[] value) {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
