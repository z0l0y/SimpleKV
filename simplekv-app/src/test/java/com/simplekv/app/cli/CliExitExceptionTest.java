package com.simplekv.app.cli;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CliExitExceptionTest {

    @Test
    void shouldExposeExitCode() {
        CliExitException exception = new CliExitException(7);
        assertEquals(7, exception.getExitCode());
    }
}
