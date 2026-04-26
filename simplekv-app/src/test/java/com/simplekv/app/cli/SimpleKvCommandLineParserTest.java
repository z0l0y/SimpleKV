package com.simplekv.app.cli;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SimpleKvCommandLineParserTest {

    @Test
    void shouldTokenizeQuotedAndEscapedArguments() {
        List<String> tokens = SimpleKvCommandLineParser.tokenize("put \"hello world\" 'more text' escaped\\ space");

        assertEquals(4, tokens.size());
        assertEquals("put", tokens.get(0));
        assertEquals("hello world", tokens.get(1));
        assertEquals("more text", tokens.get(2));
        assertEquals("escaped space", tokens.get(3));
    }

    @Test
    void shouldRejectUnterminatedQuotes() {
        assertThrows(IllegalArgumentException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() {
                SimpleKvCommandLineParser.tokenize("put \"bad");
            }
        });
    }
}
