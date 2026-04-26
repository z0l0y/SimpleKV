package com.simplekv.app.cli;

import org.springframework.boot.ExitCodeGenerator;

public final class CliExitException extends RuntimeException implements ExitCodeGenerator {
    private final int exitCode;

    public CliExitException(int exitCode) {
        super("CLI exited with code: " + exitCode);
        this.exitCode = exitCode;
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }
}
