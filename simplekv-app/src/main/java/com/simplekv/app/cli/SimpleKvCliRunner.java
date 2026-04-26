package com.simplekv.app.cli;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "simplekv", name = "mode", havingValue = "cli")
public class SimpleKvCliRunner implements ApplicationRunner {
    private final SimpleKvCli cli;

    public SimpleKvCliRunner(SimpleKvCli cli) {
        this.cli = cli;
    }

    @Override
    public void run(ApplicationArguments args) {
        int exitCode = cli.run(args.getSourceArgs(), System.out);
        if (exitCode != 0) {
            throw new CliExitException(exitCode);
        }
    }
}
