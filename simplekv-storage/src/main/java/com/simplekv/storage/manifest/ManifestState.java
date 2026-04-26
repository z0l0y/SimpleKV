package com.simplekv.storage.manifest;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;

public class ManifestState {
    private long nextFileId = 1L;
    private long lastFlushedSequence = 0L;
    private List<SstableMetadata> files = new ArrayList<>();

    public ManifestState() {
    }

    public long getNextFileId() {
        return nextFileId;
    }

    public void setNextFileId(long nextFileId) {
        this.nextFileId = nextFileId;
    }

    public long getLastFlushedSequence() {
        return lastFlushedSequence;
    }

    public void setLastFlushedSequence(long lastFlushedSequence) {
        this.lastFlushedSequence = lastFlushedSequence;
    }

    @SuppressFBWarnings(
            value = "EI_EXPOSE_REP",
            justification = "Manifest update flow intentionally mutates this list in place across modules"
    )
    public List<SstableMetadata> getFiles() {
        return files;
    }

    public void setFiles(List<SstableMetadata> files) {
        this.files = files == null ? new ArrayList<>() : new ArrayList<>(files);
    }
}
