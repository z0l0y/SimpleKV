package com.simplekv.client;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleKvConnectionPool {
    private final String host;
    private final int port;
    private final int maxConnections;
    private final Queue<SimpleKvClient> availableConnections;
    private final ReentrantLock lock;
    private int totalConnections;
    private boolean closed;

    
    public SimpleKvConnectionPool(String host, int port) {
        this(host, port, 10);
    }

    
    public SimpleKvConnectionPool(String host, int port, int maxConnections) {
        this.host = host;
        this.port = port;
        this.maxConnections = maxConnections;
        this.availableConnections = new LinkedList<>();
        this.lock = new ReentrantLock();
        this.totalConnections = 0;
        this.closed = false;
    }

    
    public SimpleKvClient getConnection() throws IOException {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Connection pool is closed");
            }

            
            SimpleKvClient client = availableConnections.poll();
            if (client != null) {
                return client;
            }

            
            if (totalConnections < maxConnections) {
                SimpleKvClient newClient = new SimpleKvClient(host, port);
                newClient.connect();
                totalConnections++;
                return newClient;
            }

            throw new IOException("Connection pool exhausted: " + maxConnections + " connections in use");
        } finally {
            lock.unlock();
        }
    }

    
    public void returnConnection(SimpleKvClient client) {
        lock.lock();
        try {
            if (!closed && client != null && client.isConnected()) {
                availableConnections.add(client);
            }
        } finally {
            lock.unlock();
        }
    }

    
    public void close() throws IOException {
        lock.lock();
        try {
            closed = true;
            SimpleKvClient client;
            while ((client = availableConnections.poll()) != null) {
                client.close();
            }
        } finally {
            lock.unlock();
        }
    }

    
    public int getAvailableCount() {
        lock.lock();
        try {
            return availableConnections.size();
        } finally {
            lock.unlock();
        }
    }

    
    public int getTotalConnections() {
        lock.lock();
        try {
            return totalConnections;
        } finally {
            lock.unlock();
        }
    }

    
    public boolean isClosed() {
        lock.lock();
        try {
            return closed;
        } finally {
            lock.unlock();
        }
    }
}
