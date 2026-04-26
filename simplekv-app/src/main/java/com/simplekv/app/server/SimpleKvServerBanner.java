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
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_MAGENTA = "\u001B[35m";

    private final String host;
    private final String dataDir;
    private final boolean bannerEnabled;

    public SimpleKvServerBanner(SimpleKvServerProperties serverProperties,
                                SimpleKvStorageProperties storageProperties) {
        this.host = serverProperties.getHost();
        this.dataDir = String.valueOf(storageProperties.getDataDir());
        this.bannerEnabled = serverProperties.isBannerEnabled();
    }

    public void print(int boundPort) {
        if (!bannerEnabled) {
            return;
        }

        long pid = ProcessHandle.current().pid();
        String version = Optional.ofNullable(SimpleKvServerBanner.class.getPackage().getImplementationVersion())
                .orElse("dev");
        String endpoint = host + ":" + boundPort;
        PrintStream out = System.out;

        out.println();
        out.println(ANSI_CYAN + "  ____  _                 _      _  __      __" + ANSI_RESET);
        out.println(ANSI_BLUE + " / ___|(_)_ __ ___  _ __ | | ___| |/ /__   / /" + ANSI_RESET);
        out.println(ANSI_MAGENTA + " \\___ \\| | '_ ` _ \\| '_ \\| |/ _ \\ ' // /  / / " + ANSI_RESET);
        out.println(ANSI_CYAN + "  ___) | | | | | | | |_) | |  __/ . \\\\ \\_/ /  " + ANSI_RESET);
        out.println(ANSI_BLUE + " |____/|_|_| |_| |_| .__/|_|\\___|_|\\_\\\\___/   " + ANSI_RESET);
        out.println(ANSI_MAGENTA + "                   |_|                         " + ANSI_RESET);
        out.println(ANSI_CYAN + "simplekv" + ANSI_RESET);
        out.println("Name      : SimpleKV");
        out.println("Version   : " + version);
        out.println("Mode      : standalone");
        out.println("Protocol  : SKSP/1.0");
        out.println("Endpoint  : " + endpoint);
        out.println("PID       : " + pid);
        out.println("Data Dir  : " + dataDir);
        out.println("Homepage  : https://github.com/z0l0y/SimpleKV");
        out.flush();

        log.info("SimpleKV server is ready: endpoint={}, protocol=SKSP/1.0, mode=standalone",
            endpoint);
        log.info("SimpleKV runtime context: version={}, pid={}, dataDir={}",
            version, Long.valueOf(pid), dataDir);
    }
}
