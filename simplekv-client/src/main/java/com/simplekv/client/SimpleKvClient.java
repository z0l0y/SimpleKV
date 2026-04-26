package com.simplekv.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SimpleKvClient implements AutoCloseable {
    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter writer;
    private BufferedReader reader;
    private boolean connected;

    public SimpleKvClient(String host, int port) {
        this.host = host;
        this.port = port;
        this.connected = false;
    }

    public SimpleKvClient(int port) {
        this("127.0.0.1", port);
    }

    
    public void connect() throws IOException {
        if (connected) {
            return;
        }
        socket = new Socket(host, port);
        writer = new PrintWriter(socket.getOutputStream(), true, StandardCharsets.UTF_8);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        connected = true;
    }

    
    public boolean isConnected() {
        return connected;
    }

    
    public SimpleKvResponse execute(String... args) throws IOException {
        if (!connected) {
            throw new IllegalStateException("Not connected to server. Call connect() first.");
        }

        
        StringBuilder command = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                command.append(" ");
            }
            String arg = args[i];
            
            if (arg.contains(" ")) {
                command.append("\"").append(arg).append("\"");
            } else {
                command.append(arg);
            }
        }

        
        writer.println(command.toString());

        
        String headerLine = reader.readLine();
        if (headerLine == null) {
            throw new IOException("Connection closed by server");
        }

        
        String[] headerParts = headerLine.split(" ");
        if (headerParts.length < 3) {
            throw new IOException("Invalid response header: " + headerLine);
        }

        String status = headerParts[1];
        int exitCode = Integer.parseInt(headerParts[2]);

        
        String countLine = reader.readLine();
        if (countLine == null) {
            throw new IOException("Connection closed by server");
        }
        int payloadCount = Integer.parseInt(countLine);

        
        List<String> payload = new ArrayList<>();
        for (int i = 0; i < payloadCount; i++) {
            String line = reader.readLine();
            if (line == null) {
                throw new IOException("Connection closed by server");
            }
            payload.add(line);
        }

        return new SimpleKvResponse(status, exitCode, payload);
    }

    
    public SimpleKvResponse set(String key, String value) throws IOException {
        return execute("SET", key, value);
    }

    
    public SimpleKvResponse get(String key) throws IOException {
        return execute("GET", key);
    }

    
    public SimpleKvResponse del(String... keys) throws IOException {
        String[] args = new String[keys.length + 1];
        args[0] = "DEL";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return execute(args);
    }

    
    public SimpleKvResponse exists(String... keys) throws IOException {
        String[] args = new String[keys.length + 1];
        args[0] = "EXISTS";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return execute(args);
    }

    
    public SimpleKvResponse lpush(String key, String... values) throws IOException {
        String[] args = new String[values.length + 2];
        args[0] = "LPUSH";
        args[1] = key;
        System.arraycopy(values, 0, args, 2, values.length);
        return execute(args);
    }

    
    public SimpleKvResponse rpush(String key, String... values) throws IOException {
        String[] args = new String[values.length + 2];
        args[0] = "RPUSH";
        args[1] = key;
        System.arraycopy(values, 0, args, 2, values.length);
        return execute(args);
    }

    
    public SimpleKvResponse lpop(String key) throws IOException {
        return execute("LPOP", key);
    }

    
    public SimpleKvResponse rpop(String key) throws IOException {
        return execute("RPOP", key);
    }

    
    public SimpleKvResponse llen(String key) throws IOException {
        return execute("LLEN", key);
    }

    
    public SimpleKvResponse lrange(String key, int start, int stop) throws IOException {
        return execute("LRANGE", key, String.valueOf(start), String.valueOf(stop));
    }

    
    public SimpleKvResponse sadd(String key, String... members) throws IOException {
        String[] args = new String[members.length + 2];
        args[0] = "SADD";
        args[1] = key;
        System.arraycopy(members, 0, args, 2, members.length);
        return execute(args);
    }

    
    public SimpleKvResponse srem(String key, String... members) throws IOException {
        String[] args = new String[members.length + 2];
        args[0] = "SREM";
        args[1] = key;
        System.arraycopy(members, 0, args, 2, members.length);
        return execute(args);
    }

    
    public SimpleKvResponse smembers(String key) throws IOException {
        return execute("SMEMBERS", key);
    }

    
    public SimpleKvResponse sismember(String key, String member) throws IOException {
        return execute("SISMEMBER", key, member);
    }

    
    public SimpleKvResponse scard(String key) throws IOException {
        return execute("SCARD", key);
    }

    
    public SimpleKvResponse hset(String key, String... fieldValues) throws IOException {
        String[] args = new String[fieldValues.length + 2];
        args[0] = "HSET";
        args[1] = key;
        System.arraycopy(fieldValues, 0, args, 2, fieldValues.length);
        return execute(args);
    }

    
    public SimpleKvResponse hget(String key, String field) throws IOException {
        return execute("HGET", key, field);
    }

    
    public SimpleKvResponse hdel(String key, String... fields) throws IOException {
        String[] args = new String[fields.length + 2];
        args[0] = "HDEL";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return execute(args);
    }

    
    public SimpleKvResponse hexists(String key, String field) throws IOException {
        return execute("HEXISTS", key, field);
    }

    
    public SimpleKvResponse hlen(String key) throws IOException {
        return execute("HLEN", key);
    }

    
    public SimpleKvResponse hgetall(String key) throws IOException {
        return execute("HGETALL", key);
    }

    
    public SimpleKvResponse ping() throws IOException {
        return execute("PING");
    }

    
    public SimpleKvResponse echo(String message) throws IOException {
        return execute("ECHO", message);
    }

    
    @Override
    public void close() throws IOException {
        if (connected) {
            try {
                if (writer != null) {
                    writer.close();
                }
                if (reader != null) {
                    reader.close();
                }
                if (socket != null) {
                    socket.close();
                }
            } finally {
                connected = false;
            }
        }
    }
}
