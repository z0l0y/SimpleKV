package com.simplekv.app.cli;

import java.util.ArrayList;
import java.util.List;

public final class SimpleKvCommandLineParser {

    private SimpleKvCommandLineParser() {
    }

    public static List<String> tokenize(String line) {
        List<String> tokens = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        char quote = 0;
        boolean escaping = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (escaping) {
                current.append(c);
                escaping = false;
                continue;
            }
            if (c == '\\') {
                escaping = true;
                continue;
            }
            if (inQuote) {
                if (c == quote) {
                    inQuote = false;
                    quote = 0;
                }
                else {
                    current.append(c);
                }
                continue;
            }
            if (c == '"' || c == '\'') {
                inQuote = true;
                quote = c;
                continue;
            }
            if (Character.isWhitespace(c)) {
                if (current.length() > 0) {
                    tokens.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }
            current.append(c);
        }

        if (escaping) {
            current.append('\\');
        }
        if (inQuote) {
            throw new IllegalArgumentException("unterminated quoted string");
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }
}