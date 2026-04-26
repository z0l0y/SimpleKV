package com.simplekv.core;

import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.options.StorageOptions;

import java.io.IOException;

public final class SimpleKvEngines {
    private SimpleKvEngines() {
    }

    public static KeyValueStore open(StorageOptions options) throws IOException {
        return SimpleKvEngine.open(options);
    }
}
