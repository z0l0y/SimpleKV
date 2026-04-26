package com.simplekv.app;

import com.simplekv.app.config.SimpleKvServerProperties;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.app.nativeimage.SimpleKvRuntimeHints;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ImportRuntimeHints;

@SpringBootApplication
@EnableConfigurationProperties({SimpleKvStorageProperties.class, SimpleKvServerProperties.class})
@ImportRuntimeHints(SimpleKvRuntimeHints.class)
public class SimpleKvApplication {
    public static void main(String[] args) {
        SpringApplication.run(SimpleKvApplication.class, args);
    }
}
