package com.simplekv.api.options;

import java.util.Locale;

public enum CompactionStyle {
    LEVELED,
    SIZE_TIERED;

    public static CompactionStyle parse(String rawStyle) {
        if (rawStyle == null || rawStyle.trim().isEmpty()) {
            return LEVELED;
        }
        String normalized = rawStyle.trim().toLowerCase(Locale.ROOT).replace('_', '-');
        return switch (normalized) {
            case "leveled" -> LEVELED;
            case "size-tiered", "size-tired" -> SIZE_TIERED;
            default -> throw new IllegalArgumentException("Unsupported compaction style: " + rawStyle);
        };
    }
}
