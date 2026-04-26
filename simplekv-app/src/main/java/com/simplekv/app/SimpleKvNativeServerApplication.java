package com.simplekv.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.app.cli.SimpleKvCli;
import com.simplekv.app.config.SimpleKvConfiguration;
import com.simplekv.app.config.SimpleKvServerProperties;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.app.diag.DiagnosticService;
import com.simplekv.app.server.SimpleKvServerBanner;
import com.simplekv.app.server.SimpleKvSkspServer;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

public final class SimpleKvNativeServerApplication {

    private static final String STORAGE_PREFIX = "simplekv.storage.";
    private static final String SERVER_PREFIX = "simplekv.server.";

    private SimpleKvNativeServerApplication() {
    }

    public static void main(String[] args) throws IOException {
        SimpleKvStorageProperties storage = new SimpleKvStorageProperties();
        SimpleKvServerProperties server = new SimpleKvServerProperties();

        applySystemPropertyOverrides(storage, server);
        boolean explicitDataDir = hasExplicitDataDirSetting(args)
                || hasText(resolveValue("simplekv.storage.data-dir"));
        if (!explicitDataDir && isDefaultDataDir(storage.getDataDir())) {
            Path workingDirectory = Paths.get("").toAbsolutePath().normalize();
            storage.setDataDir(SimpleKvNativeApplication.resolveDefaultDataDir(workingDirectory));
        }
        applyCommandLineOverrides(storage, server, args);

        SimpleKvConfiguration configuration = new SimpleKvConfiguration();
        ObjectMapper objectMapper = configuration.objectMapper();
        StorageOptions storageOptions = configuration.storageOptions(storage);

        try (KeyValueStore store = configuration.keyValueStore(storageOptions)) {
            DiagnosticService diagnosticService = new DiagnosticService(storage);
            SimpleKvCli cli = new SimpleKvCli(store, diagnosticService, objectMapper, storage);
            SimpleKvServerBanner banner = new SimpleKvServerBanner(server, storage);
            SimpleKvSkspServer skspServer = new SimpleKvSkspServer(cli, store, server, banner);

            registerShutdownHook(skspServer);
            skspServer.start();
        }
    }

    private static void registerShutdownHook(SimpleKvSkspServer server) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    server.close();
                }
                catch (IOException ignored) {
                    
                }
            }
        }));
    }

    private static void applySystemPropertyOverrides(SimpleKvStorageProperties storage,
            SimpleKvServerProperties server) {
        applyStorageProperty(storage, "simplekv.storage.data-dir", resolveValue("simplekv.storage.data-dir"));
        applyStorageProperty(storage, "simplekv.storage.mem-table-max-entries",
                resolveValue("simplekv.storage.mem-table-max-entries"));
        applyStorageProperty(storage, "simplekv.storage.scan-default-limit",
                resolveValue("simplekv.storage.scan-default-limit"));
        applyStorageProperty(storage, "simplekv.storage.cache-max-entries",
                resolveValue("simplekv.storage.cache-max-entries"));
        applyStorageProperty(storage, "simplekv.storage.bloom-filter-enabled",
                resolveValue("simplekv.storage.bloom-filter-enabled"));
        applyStorageProperty(storage, "simplekv.storage.bloom-false-positive-rate",
                resolveValue("simplekv.storage.bloom-false-positive-rate"));
        applyStorageProperty(storage, "simplekv.storage.level0-compaction-trigger",
                resolveValue("simplekv.storage.level0-compaction-trigger"));
        applyStorageProperty(storage, "simplekv.storage.level0-max-files",
                resolveValue("simplekv.storage.level0-max-files"));
        applyStorageProperty(storage, "simplekv.storage.flush-batch-size",
                resolveValue("simplekv.storage.flush-batch-size"));
        applyStorageProperty(storage, "simplekv.storage.tombstone-retention-seconds",
                resolveValue("simplekv.storage.tombstone-retention-seconds"));
        applyStorageProperty(storage, "simplekv.storage.compaction-style",
                resolveValue("simplekv.storage.compaction-style"));

        applyServerProperty(server, "simplekv.server.host", resolveValue("simplekv.server.host"));
        applyServerProperty(server, "simplekv.server.port", resolveValue("simplekv.server.port"));
        applyServerProperty(server, "simplekv.server.backlog", resolveValue("simplekv.server.backlog"));
        applyServerProperty(server, "simplekv.server.banner-enabled",
                resolveValue("simplekv.server.banner-enabled"));
    }

    private static void applyCommandLineOverrides(SimpleKvStorageProperties storage,
            SimpleKvServerProperties server,
            String[] args) {
        if (args == null) {
            return;
        }
        for (String arg : args) {
            if (arg == null) {
                continue;
            }
            String candidate = arg;
            if (candidate.startsWith("-D")) {
                candidate = candidate.substring(2);
            }
            else if (candidate.startsWith("--")) {
                candidate = candidate.substring(2);
            }
            else {
                continue;
            }
            int equals = candidate.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String name = candidate.substring(0, equals);
            String value = candidate.substring(equals + 1);
            if (name.startsWith(STORAGE_PREFIX)) {
                applyStorageProperty(storage, name, value);
            }
            else if (name.startsWith(SERVER_PREFIX)) {
                applyServerProperty(server, name, value);
            }
        }
    }

    private static void applyStorageProperty(SimpleKvStorageProperties storage, String name, String value) {
        if (!hasText(value)) {
            return;
        }
        if ("simplekv.storage.data-dir".equals(name)) {
            storage.setDataDir(Paths.get(value));
        }
        else if ("simplekv.storage.mem-table-max-entries".equals(name)) {
            storage.setMemTableMaxEntries(Integer.parseInt(value));
        }
        else if ("simplekv.storage.scan-default-limit".equals(name)) {
            storage.setScanDefaultLimit(Integer.parseInt(value));
        }
        else if ("simplekv.storage.cache-max-entries".equals(name)) {
            storage.setCacheMaxEntries(Integer.parseInt(value));
        }
        else if ("simplekv.storage.bloom-filter-enabled".equals(name)) {
            storage.setBloomFilterEnabled(Boolean.parseBoolean(value));
        }
        else if ("simplekv.storage.bloom-false-positive-rate".equals(name)) {
            storage.setBloomFalsePositiveRate(Double.parseDouble(value));
        }
        else if ("simplekv.storage.level0-compaction-trigger".equals(name)) {
            storage.setLevel0CompactionTrigger(Integer.parseInt(value));
        }
        else if ("simplekv.storage.level0-max-files".equals(name)) {
            storage.setLevel0MaxFiles(Integer.parseInt(value));
        }
        else if ("simplekv.storage.flush-batch-size".equals(name)) {
            storage.setFlushBatchSize(Integer.parseInt(value));
        }
        else if ("simplekv.storage.tombstone-retention-seconds".equals(name)) {
            storage.setTombstoneRetentionSeconds(Integer.parseInt(value));
        }
        else if ("simplekv.storage.compaction-style".equals(name)) {
            storage.setCompactionStyle(value);
        }
    }

    private static void applyServerProperty(SimpleKvServerProperties server, String name, String value) {
        if (!hasText(value)) {
            return;
        }
        if ("simplekv.server.host".equals(name)) {
            server.setHost(value);
        }
        else if ("simplekv.server.port".equals(name)) {
            server.setPort(Integer.parseInt(value));
        }
        else if ("simplekv.server.backlog".equals(name)) {
            server.setBacklog(Integer.parseInt(value));
        }
        else if ("simplekv.server.banner-enabled".equals(name)) {
            server.setBannerEnabled(Boolean.parseBoolean(value));
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

    private static boolean isDefaultDataDir(Path dataDir) {
        String raw = dataDir == null ? "" : dataDir.toString();
        return "./data".equals(raw) || "data".equals(raw) || ".\\data".equals(raw);
    }
}
