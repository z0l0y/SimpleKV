package com.simplekv.storage.model;

import java.util.Objects;

public final class InternalEntry {
	private final InternalKey key;
	private final InternalValue value;

	public InternalEntry(InternalKey key, InternalValue value) {
		this.key = Objects.requireNonNull(key, "key");
		this.value = Objects.requireNonNull(value, "value");
	}

	public InternalKey key() {
		return key;
	}

	public InternalValue value() {
		return value;
	}
}
