package com.simplekv.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.app.cli.SimpleKvCli;
import com.simplekv.app.config.SimpleKvConfiguration;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.app.diag.DiagnosticService;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class SimpleKvNativeApplication {

    private static final String PROPERTY_PREFIX = "simplekv.storage.";

    private SimpleKvNativeApplication() {
    }

    public static void main(String[] args) throws IOException {
        SimpleKvStorageProperties properties = new SimpleKvStorageProperties();
        applySystemPropertyOverrides(properties);

        boolean explicitDataDir = hasExplicitDataDirSetting(args)
                || hasText(resolveValue("simplekv.storage.data-dir"));
        if (!explicitDataDir && isDefaultDataDir(properties.getDataDir())) {
            Path workingDirectory = Paths.get("").toAbsolutePath().normalize();
            properties.setDataDir(resolveDefaultDataDir(workingDirectory));
        }

        List<String> cliArguments = applyCommandLineOverrides(properties, args);

        
        if (cliArguments.isEmpty()) {
            cliArguments.add("repl");
        }

        SimpleKvConfiguration configuration = new SimpleKvConfiguration();
        ObjectMapper objectMapper = configuration.objectMapper();
        StorageOptions storageOptions = configuration.storageOptions(properties);

        try (KeyValueStore store = configuration.keyValueStore(storageOptions)) {
            DiagnosticService diagnosticService = new DiagnosticService(properties);
            SimpleKvCli cli = new SimpleKvCli(store, diagnosticService, objectMapper, properties);
            int exitCode = cli.run(cliArguments.toArray(new String[0]), System.out);
            if (exitCode != 0) {
                System.exit(exitCode);
            }
        }
    }

    private static void applySystemPropertyOverrides(SimpleKvStorageProperties properties) {
        applyDataDir(properties, resolveValue("simplekv.storage.data-dir"));
        applyInt(properties, resolveValue("simplekv.storage.mem-table-max-entries"), properties::setMemTableMaxEntries);
        applyInt(properties, resolveValue("simplekv.storage.scan-default-limit"), properties::setScanDefaultLimit);
        applyInt(properties, resolveValue("simplekv.storage.cache-max-entries"), properties::setCacheMaxEntries);
        applyBoolean(properties, resolveValue("simplekv.storage.bloom-filter-enabled"),
                properties::setBloomFilterEnabled);
        applyDouble(properties, resolveValue("simplekv.storage.bloom-false-positive-rate"),
                properties::setBloomFalsePositiveRate);
        applyInt(properties, resolveValue("simplekv.storage.level0-compaction-trigger"),
                properties::setLevel0CompactionTrigger);
        applyInt(properties, resolveValue("simplekv.storage.level0-max-files"), properties::setLevel0MaxFiles);
        applyInt(properties, resolveValue("simplekv.storage.flush-batch-size"), properties::setFlushBatchSize);
        applyInt(properties, resolveValue("simplekv.storage.tombstone-retention-seconds"),
                properties::setTombstoneRetentionSeconds);
        applyString(properties, resolveValue("simplekv.storage.compaction-style"),
                properties::setCompactionStyle);
    }

    private static List<String> applyCommandLineOverrides(SimpleKvStorageProperties properties, String[] args) {
        List<String> cliArguments = new ArrayList<String>();
        if (args == null) {
            return cliArguments;
        }
        for (String arg : args) {
            if ("--".equals(arg)) {
                continue;
            }
            if (applyCommandLineOverride(properties, arg)) {
                continue;
            }
            cliArguments.add(arg);
        }
        return cliArguments;
    }

    private static boolean applyCommandLineOverride(SimpleKvStorageProperties properties, String arg) {
        if (arg == null || !arg.startsWith("-") || arg.length() < 3) {
            return false;
        }
        String candidate = arg;
        if (candidate.startsWith("-D")) {
            candidate = candidate.substring(2);
        }
        else if (candidate.startsWith("--")) {
            candidate = candidate.substring(2);
        }
        else {
            return false;
        }
        int equals = candidate.indexOf('=');
        if (equals <= 0) {
            return false;
        }
        String name = candidate.substring(0, equals);
        String value = candidate.substring(equals + 1);
        if (!name.startsWith(PROPERTY_PREFIX)) {
            return false;
        }
        applyProperty(properties, name, value);
        return true;
    }

    private static void applyProperty(SimpleKvStorageProperties properties, String name, String value) {
        if ("simplekv.storage.data-dir".equals(name)) {
            applyDataDir(properties, value);
        }
        else if ("simplekv.storage.mem-table-max-entries".equals(name)) {
            applyInt(properties, value, properties::setMemTableMaxEntries);
        }
        else if ("simplekv.storage.scan-default-limit".equals(name)) {
            applyInt(properties, value, properties::setScanDefaultLimit);
        }
        else if ("simplekv.storage.cache-max-entries".equals(name)) {
            applyInt(properties, value, properties::setCacheMaxEntries);
        }
        else if ("simplekv.storage.bloom-filter-enabled".equals(name)) {
            applyBoolean(properties, value, properties::setBloomFilterEnabled);
        }
        else if ("simplekv.storage.bloom-false-positive-rate".equals(name)) {
            applyDouble(properties, value, properties::setBloomFalsePositiveRate);
        }
        else if ("simplekv.storage.level0-compaction-trigger".equals(name)) {
            applyInt(properties, value, properties::setLevel0CompactionTrigger);
        }
        else if ("simplekv.storage.level0-max-files".equals(name)) {
            applyInt(properties, value, properties::setLevel0MaxFiles);
        }
        else if ("simplekv.storage.flush-batch-size".equals(name)) {
            applyInt(properties, value, properties::setFlushBatchSize);
        }
        else if ("simplekv.storage.tombstone-retention-seconds".equals(name)) {
            applyInt(properties, value, properties::setTombstoneRetentionSeconds);
        }
        else if ("simplekv.storage.compaction-style".equals(name)) {
            applyString(properties, value, properties::setCompactionStyle);
        }
    }

    private static String resolveValue(String propertyName) {
        String value = System.getProperty(propertyName);
        if (value != null) {
            return value;
        }
        value = System.getenv(propertyName);
        if (value != null) {
            return value;
        }
        String environmentName = propertyName.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_');
        return System.getenv(environmentName);
    }

    private static void applyDataDir(SimpleKvStorageProperties properties, String value) {
        if (value != null && !value.isEmpty()) {
            properties.setDataDir(Paths.get(value));
        }
    }

    static Path resolveDefaultDataDir(Path workingDirectory) {
        List<Path> candidates = new ArrayList<Path>();
        candidates.add(workingDirectory.resolve("data"));

        Path parent = workingDirectory.getParent();
        if (parent != null) {
            candidates.add(parent.resolve("data"));

            Path grandParent = parent.getParent();
            if (grandParent != null) {
                candidates.add(grandParent.resolve("data"));
            }
        }

        for (Path candidate : candidates) {
            if (looksLikeSimpleKvDataDir(candidate)) {
                return candidate.normalize();
            }
        }
        return workingDirectory.resolve("data").normalize();
    }

    private static boolean looksLikeSimpleKvDataDir(Path dir) {
        return Files.isDirectory(dir)
                && (Files.exists(dir.resolve("MANIFEST.json")) || Files.exists(dir.resolve("CURRENT")));
    }

    private static boolean isDefaultDataDir(Path dataDir) {
        String raw = dataDir == null ? "" : dataDir.toString();
        return "./data".equals(raw) || "data".equals(raw) || ".\\data".equals(raw);
    }

    private static boolean hasExplicitDataDirSetting(String[] args) {
        if (args == null) {
            return false;
        }
        for (String arg : args) {
            if (arg == null) {
                continue;
            }
            if (arg.startsWith("-Dsimplekv.storage.data-dir=") || arg.startsWith("--simplekv.storage.data-dir=")) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isEmpty();
    }

    private static void applyInt(SimpleKvStorageProperties properties, String value, IntPropertySetter setter) {
        if (value != null && !value.isEmpty()) {
            setter.set(Integer.parseInt(value));
        }
    }

    private static void applyBoolean(SimpleKvStorageProperties properties, String value,
            BooleanPropertySetter setter) {
        if (value != null && !value.isEmpty()) {
            setter.set(Boolean.parseBoolean(value));
        }
    }

    private static void applyDouble(SimpleKvStorageProperties properties, String value, DoublePropertySetter setter) {
        if (value != null && !value.isEmpty()) {
            setter.set(Double.parseDouble(value));
        }
    }

    private static void applyString(SimpleKvStorageProperties properties, String value, StringPropertySetter setter) {
        if (value != null && !value.isEmpty()) {
            setter.set(value);
        }
    }

    private interface IntPropertySetter {
        void set(int value);
    }

    private interface BooleanPropertySetter {
        void set(boolean value);
    }

    private interface DoublePropertySetter {
        void set(double value);
    }

    private interface StringPropertySetter {
        void set(String value);
    }

}
