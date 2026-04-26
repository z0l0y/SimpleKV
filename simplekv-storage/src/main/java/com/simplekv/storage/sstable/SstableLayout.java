package com.simplekv.storage.sstable;

public final class SstableLayout {
    public static final int MAGIC = 0x534B5654;
    public static final int VERSION = 1;
    public static final int FOOTER_MAGIC = 0x534B5646;

    public static final int HEADER_BYTES = Integer.BYTES + Integer.BYTES;
    public static final int FOOTER_BYTES = Long.BYTES + Integer.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES;

    private SstableLayout() {
    }
}
