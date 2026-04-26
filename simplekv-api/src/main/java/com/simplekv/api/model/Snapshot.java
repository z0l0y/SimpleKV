package com.simplekv.api.model;

public final class Snapshot {
	private final long sequence;

	public Snapshot(long sequence) {
		this.sequence = sequence;
	}

	public long getSequence() {
		return sequence;
	}
}
