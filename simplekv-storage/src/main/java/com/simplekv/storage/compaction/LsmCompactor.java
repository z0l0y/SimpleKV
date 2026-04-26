package com.simplekv.storage.compaction;

import com.simplekv.api.options.CompactionStyle;
import com.simplekv.storage.model.InternalEntry;
import com.simplekv.storage.model.ValueType;
import com.simplekv.storage.sstable.SstableReader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class LsmCompactor {
    private final CompactionStyle style;

    public LsmCompactor() {
        this(CompactionStyle.LEVELED);
    }

    public LsmCompactor(CompactionStyle style) {
        this.style = style != null ? style : CompactionStyle.LEVELED;
    }

    public List<InternalEntry> compact(List<SstableReader> readers) {
        return compact(readers, System.currentTimeMillis(), false);
    }

    public List<InternalEntry> compact(List<SstableReader> readers, long nowMillis, boolean dropDeletedAndExpired) {
        return switch (style) {
            case LEVELED -> compactLeveled(readers, nowMillis, dropDeletedAndExpired);
            case SIZE_TIERED -> compactSizeTiered(readers, nowMillis, dropDeletedAndExpired);
        };
    }

    
    private List<InternalEntry> compactLeveled(List<SstableReader> readers, long nowMillis, boolean dropDeletedAndExpired) {
        List<InternalEntry> merged = new ArrayList<>();
        for (SstableReader reader : readers) {
            merged.addAll(reader.allEntries());
        }
        merged.sort(Comparator.comparing(InternalEntry::key));

        if (merged.isEmpty()) {
            return merged;
        }

        return deduplicateAndFilter(merged, nowMillis, dropDeletedAndExpired);
    }

    
    private List<InternalEntry> compactSizeTiered(List<SstableReader> readers, long nowMillis, boolean dropDeletedAndExpired) {
        if (readers.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<InternalEntry> merged = new ArrayList<>();
        for (SstableReader reader : readers) {
            merged.addAll(reader.allEntries());
        }
        merged.sort(Comparator.comparing(InternalEntry::key));

        if (merged.isEmpty()) {
            return merged;
        }

        return deduplicateAndFilter(merged, nowMillis, dropDeletedAndExpired);
    }

    
    private List<InternalEntry> deduplicateAndFilter(List<InternalEntry> sorted, long nowMillis, boolean dropDeletedAndExpired) {
        List<InternalEntry> deduped = new ArrayList<>(sorted.size());
        String previousUserKey = null;
        
        for (InternalEntry entry : sorted) {
            String userKey = entry.key().userKey();
            if (previousUserKey != null && previousUserKey.equals(userKey)) {
                continue;
            }

            previousUserKey = userKey;
            if (dropDeletedAndExpired) {
                if (entry.key().valueType() == ValueType.DELETE) {
                    continue;
                }
                if (entry.value().isExpired(nowMillis)) {
                    continue;
                }
            }

            deduped.add(entry);
        }
        return deduped;
    }
}
