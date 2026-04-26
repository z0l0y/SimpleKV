package com.simplekv.client;

import java.util.Collections;
import java.util.List;

public class SimpleKvResponse {
    private final String status;
    private final int exitCode;
    private final List<String> payload;

    public SimpleKvResponse(String status, int exitCode, List<String> payload) {
        this.status = status;
        this.exitCode = exitCode;
        this.payload = Collections.unmodifiableList(payload);
    }

    
    public String getStatus() {
        return status;
    }

    
    public boolean isOk() {
        return "OK".equals(status) && exitCode == 0;
    }

    
    public int getExitCode() {
        return exitCode;
    }

    
    public List<String> getPayload() {
        return payload;
    }

    
    public String getFirst() {
        return payload.isEmpty() ? null : payload.get(0);
    }

    
    public int size() {
        return payload.size();
    }

    
    public boolean isEmpty() {
        return payload.isEmpty();
    }

    @Override
    public String toString() {
        return "SimpleKvResponse{" +
                "status='" + status + '\'' +
                ", exitCode=" + exitCode +
                ", payload=" + payload +
                '}';
    }
}
