package com.simplekv.app.config;

import com.simplekv.api.options.CompactionStyle;
import com.simplekv.api.options.FsyncPolicy;
import com.simplekv.api.store.KeyValueStore;
import com.simplekv.api.options.StorageOptions;
import com.simplekv.core.SimpleKvEngines;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration(proxyBeanMethods = false)
public class SimpleKvConfiguration {

    @Bean
    public ObjectMapper objectMapper() {
        return JsonMapper.builder().build();
    }

    @Bean(destroyMethod = "close")
    public KeyValueStore keyValueStore(StorageOptions options) throws IOException {
        return SimpleKvEngines.open(options);
    }

    @Bean
    public StorageOptions storageOptions(SimpleKvStorageProperties properties) {
        int bloomBits = properties.isBloomFilterEnabled()
                ? bloomBitsPerKey(properties.getBloomFalsePositiveRate())
                : 1;

        return StorageOptions.builder(properties.getDataDir())
                .mutableMemtableMaxEntries(properties.getMemTableMaxEntries())
                .dataBlockMaxEntries(Math.max(32, properties.getFlushBatchSize()))
                .l0CompactionTrigger(properties.getLevel0CompactionTrigger())
                .l0StopWritesTrigger(Math.max(properties.getLevel0MaxFiles(), properties.getLevel0CompactionTrigger() + 1))
                .bloomFilterBitsPerKey(bloomBits)
                .blockCacheMaxEntries(properties.getCacheMaxEntries())
                .backpressureSleepMillis(10L)
                .backpressureMaxRetries(100)
                .backgroundCompactionEnabled(true)
                .periodicCleanupIntervalMillis(Math.max(1000L, properties.getTombstoneRetentionSeconds() * 1000L))
                .slowQueryThresholdMillis(20L)
                .fsyncPolicy(FsyncPolicy.EVERY_N_MILLIS)
                .fsyncEveryMillis(50L)
                .compactionStyle(CompactionStyle.parse(properties.getCompactionStyle()))
                .build();
    }

    private static int bloomBitsPerKey(double falsePositiveRate) {
        double p = falsePositiveRate;
        if (p <= 0d || p >= 1d) {
            p = 0.01d;
        }
        return Math.max(1, (int) Math.ceil((-Math.log(p)) / (Math.log(2d) * Math.log(2d))));
    }

}
