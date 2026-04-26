package com.simplekv.storage.manifest;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.simplekv.storage.StorageLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;

public final class ManifestStore {
    private final Path dataDir;
    private final Path currentFile;
    private final ObjectMapper objectMapper;

    public ManifestStore(Path dataDir) {
        this.dataDir = dataDir;
        this.currentFile = StorageLayout.currentFile(dataDir);
        this.objectMapper = new ObjectMapper()
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .enable(SerializationFeature.INDENT_OUTPUT);
    }

    public ManifestState loadOrCreate() throws IOException {
        Path manifestFile = resolveManifestFile();
        if (!Files.exists(manifestFile)) {
            ManifestState state = new ManifestState();
            save(state);
            return state;
        }
        return objectMapper.readValue(manifestFile.toFile(), ManifestState.class);
    }

    public void save(ManifestState state) throws IOException {
        Path manifestFile = StorageLayout.manifestFile(dataDir);
        Files.createDirectories(dataDir);
        String manifestFileName = fileNameOf(manifestFile);
        Path tempFile = manifestFile.resolveSibling(manifestFileName + ".tmp");
        objectMapper.writeValue(tempFile.toFile(), state);
        Files.move(tempFile, manifestFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Files.write(currentFile,
            Arrays.asList(manifestFileName),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    private Path resolveManifestFile() throws IOException {
        if (!Files.exists(currentFile)) {
            return StorageLayout.manifestFile(dataDir);
        }
        List<String> lines = Files.readAllLines(currentFile);
        if (lines.isEmpty() || lines.get(0).trim().isEmpty()) {
            return StorageLayout.manifestFile(dataDir);
        }
        return dataDir.resolve(lines.get(0).trim());
    }

    private static String fileNameOf(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            return path.toString();
        }
        return fileName.toString();
    }
}
