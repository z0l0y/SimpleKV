package com.simplekv.app.server;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "simplekv", name = "mode", havingValue = "server", matchIfMissing = true)
public class SimpleKvServerRunner implements ApplicationRunner {
    private final SimpleKvSkspServer server;

    public SimpleKvServerRunner(SimpleKvSkspServer server) {
        this.server = server;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        server.start();
    }
}