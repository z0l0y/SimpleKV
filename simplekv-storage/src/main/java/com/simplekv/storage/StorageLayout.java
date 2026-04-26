package com.simplekv.storage;

import java.nio.file.Path;

public final class StorageLayout {
    public static final String WAL_FILE = "wal.log";
    public static final String CURRENT_FILE = "CURRENT";
    public static final String MANIFEST_FILE = "MANIFEST.json";

    private StorageLayout() {
    }

    public static Path walFile(Path dataDir) {
        return dataDir.resolve(WAL_FILE);
    }

    public static Path manifestFile(Path dataDir) {
        return dataDir.resolve(MANIFEST_FILE);
    }

    public static Path currentFile(Path dataDir) {
        return dataDir.resolve(CURRENT_FILE);
    }

    public static Path sstableFile(Path dataDir, long fileId, int level) {
        return dataDir.resolve("sst-" + fileId + "-L" + level + ".sst");
    }

    public static Path bloomFileForSstable(Path sstableFile) {
        Path fileName = sstableFile.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException("sstableFile must include a file name");
        }
        return sstableFile.resolveSibling(fileName.toString() + ".bf");
    }
}
