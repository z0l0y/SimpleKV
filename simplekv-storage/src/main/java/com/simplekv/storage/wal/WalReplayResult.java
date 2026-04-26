package com.simplekv.storage.wal;

public final class WalReplayResult {
	private final long maxSequence;
	private final int appliedRecords;

	public WalReplayResult(long maxSequence, int appliedRecords) {
		this.maxSequence = maxSequence;
		this.appliedRecords = appliedRecords;
	}

	public long maxSequence() {
		return maxSequence;
	}

	public int appliedRecords() {
		return appliedRecords;
	}
}
