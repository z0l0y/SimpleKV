package com.simplekv.app.server;

import com.simplekv.app.config.SimpleKvServerProperties;
import com.simplekv.app.config.SimpleKvStorageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.PrintStream;
import java.util.Optional;

@Component
public class SimpleKvServerBanner {
    private static final Logger log = LoggerFactory.getLogger(SimpleKvServerBanner.class);

    private final SimpleKvServerProperties serverProperties;
    private final SimpleKvStorageProperties storageProperties;

    public SimpleKvServerBanner(SimpleKvServerProperties serverProperties,
                                SimpleKvStorageProperties storageProperties) {
        this.serverProperties = serverProperties;
        this.storageProperties = storageProperties;
    }

    public void print(int boundPort) {
        if (!serverProperties.isBannerEnabled()) {
            return;
        }

        long pid = ProcessHandle.current().pid();
        String version = Optional.ofNullable(SimpleKvServerBanner.class.getPackage().getImplementationVersion())
                .orElse("dev");
        String host = serverProperties.getHost();
        String dataDir = String.valueOf(storageProperties.getDataDir());
        PrintStream out = System.out;

        out.println();
        out.println("+---------------------------------------------------------------+");
        out.println("|                       SIMPLEKV SERVER                         |");
        out.println("+---------------------------------------------------------------+");
        out.println("|   _____ _                 _      _  __ __      __            |");
        out.println("|  / ___/(_)___ ___  ____  | | __ | |/ // /___ _/ /_           |");
        out.println("|  \\__ \\/ / __ `__ \\/ __ \\ | |/ / |   // / __ `/ __/           |");
        out.println("| ___/ / / / / / / / /_/ / |   <  /   |/ / /_/ / /_            |");
        out.println("|/____/_/_/ /_/ /_/ .___/  |_|\\_\\/_/|_/_/\\__,_/\\__/            |");
        out.println("|                 /_/                                           |");
        out.println("+---------------------------------------------------------------+");
        out.println("Name      : SimpleKV");
        out.println("Version   : " + version);
        out.println("Mode      : standalone");
        out.println("Protocol  : SKSP/1.0");
        out.println("Endpoint  : " + host + ":" + boundPort);
        out.println("PID       : " + pid);
        out.println("Data Dir  : " + dataDir);
        out.println("Homepage  : https://github.com/z0l0y/SimpleKV");
        out.flush();

        log.info("SimpleKV server is ready: endpoint={}, protocol=SKSP/1.0, mode=standalone",
            host + ":" + boundPort);
        log.info("SimpleKV runtime context: version={}, pid={}, dataDir={}",
            version, Long.valueOf(pid), dataDir);
    }
}
