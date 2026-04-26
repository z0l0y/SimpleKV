package com.simplekv.app.cli;

import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SimpleKvCliRunnerTest {

    @Test
    void shouldRunCliSuccessfully() {
        SimpleKvCli cli = mock(SimpleKvCli.class);
        when(cli.run(any(String[].class), eq(System.out))).thenReturn(0);

        SimpleKvCliRunner runner = new SimpleKvCliRunner(cli);
        runner.run(new DefaultApplicationArguments(new String[]{"stats"}));
    }

    @Test
    void shouldThrowCliExitExceptionOnFailure() {
        SimpleKvCli cli = mock(SimpleKvCli.class);
        when(cli.run(any(String[].class), eq(System.out))).thenReturn(3);

        SimpleKvCliRunner runner = new SimpleKvCliRunner(cli);
        assertThrows(CliExitException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws Throwable {
                runner.run(new DefaultApplicationArguments(new String[]{"stats"}));
            }
        });
    }
}
