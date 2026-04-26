package com.simplekv.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class SimpleKvClientTest {
    private SimpleKvClient client;

    @BeforeEach
    void setUp() throws IOException {
        client = new SimpleKvClient(7379);
        try {
            client.connect();
        } catch (IOException e) {
            
            throw new RuntimeException("SimpleKV server is not running on 127.0.0.1:7379. " +
                    "Start the server before running integration tests.", e);
        }
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void shouldConnectToServer() {
        assertTrue(client.isConnected());
    }

    @Test
    void shouldExecutePingCommand() throws IOException {
        SimpleKvResponse response = client.ping();
        assertTrue(response.isOk());
        assertEquals("PONG", response.getFirst());
    }

    @Test
    void shouldExecuteEchoCommand() throws IOException {
        SimpleKvResponse response = client.echo("Hello World");
        assertTrue(response.isOk());
        assertEquals("Hello World", response.getFirst());
    }

    @Test
    void shouldSetAndGetValues() throws IOException {
        
        SimpleKvResponse setResponse = client.set("test-key", "test-value");
        assertTrue(setResponse.isOk());

        
        SimpleKvResponse getResponse = client.get("test-key");
        assertTrue(getResponse.isOk());
        assertEquals("test-value", getResponse.getFirst());

        
        client.del("test-key");
    }

    @Test
    void shouldHandleDeleteOperation() throws IOException {
        
        client.set("key1", "value1");
        client.set("key2", "value2");

        
        SimpleKvResponse delResponse = client.del("key1", "key2");
        assertTrue(delResponse.isOk());

        
        SimpleKvResponse getResponse = client.get("key1");
        assertTrue(getResponse.isOk());
        assertNull(getResponse.getFirst());
    }

    @Test
    void shouldHandleExistsOperation() throws IOException {
        client.set("exists-key", "value");

        SimpleKvResponse response = client.exists("exists-key", "non-exists-key");
        assertTrue(response.isOk());
        assertEquals(2, response.size());

        client.del("exists-key");
    }

    @Test
    void shouldHandleListOperations() throws IOException {
        String listKey = "test-list";

        
        SimpleKvResponse lpushResponse = client.lpush(listKey, "a", "b", "c");
        assertTrue(lpushResponse.isOk());
        assertEquals("3", lpushResponse.getFirst());

        
        SimpleKvResponse llenResponse = client.llen(listKey);
        assertTrue(llenResponse.isOk());
        assertEquals("3", llenResponse.getFirst());

        
        SimpleKvResponse lrangeResponse = client.lrange(listKey, 0, -1);
        assertTrue(lrangeResponse.isOk());
        assertEquals(3, lrangeResponse.size());
        assertEquals("c", lrangeResponse.getPayload().get(0));
        assertEquals("b", lrangeResponse.getPayload().get(1));
        assertEquals("a", lrangeResponse.getPayload().get(2));

        
        SimpleKvResponse lpopResponse = client.lpop(listKey);
        assertTrue(lpopResponse.isOk());
        assertEquals("c", lpopResponse.getFirst());

        
        SimpleKvResponse rpopResponse = client.rpop(listKey);
        assertTrue(rpopResponse.isOk());
        assertEquals("a", rpopResponse.getFirst());

        
        client.del(listKey);
    }

    @Test
    void shouldHandleSetOperations() throws IOException {
        String setKey = "test-set";

        
        SimpleKvResponse saddResponse = client.sadd(setKey, "red", "green", "blue");
        assertTrue(saddResponse.isOk());
        assertEquals("3", saddResponse.getFirst());

        
        SimpleKvResponse scardResponse = client.scard(setKey);
        assertTrue(scardResponse.isOk());
        assertEquals("3", scardResponse.getFirst());

        
        SimpleKvResponse sismemberResponse = client.sismember(setKey, "red");
        assertTrue(sismemberResponse.isOk());
        assertEquals("1", sismemberResponse.getFirst());

        
        SimpleKvResponse smembersResponse = client.smembers(setKey);
        assertTrue(smembersResponse.isOk());
        assertEquals(3, smembersResponse.size());

        
        SimpleKvResponse sremResponse = client.srem(setKey, "red");
        assertTrue(sremResponse.isOk());
        assertEquals("1", sremResponse.getFirst());

        
        client.del(setKey);
    }

    @Test
    void shouldHandleHashOperations() throws IOException {
        String hashKey = "test-hash";

        
        SimpleKvResponse hsetResponse = client.hset(hashKey, "name", "Alice", "age", "30", "city", "Shanghai");
        assertTrue(hsetResponse.isOk());
        assertEquals("3", hsetResponse.getFirst());

        
        SimpleKvResponse hgetResponse = client.hget(hashKey, "name");
        assertTrue(hgetResponse.isOk());
        assertEquals("Alice", hgetResponse.getFirst());

        
        SimpleKvResponse hexistsResponse = client.hexists(hashKey, "age");
        assertTrue(hexistsResponse.isOk());
        assertEquals("1", hexistsResponse.getFirst());

        
        SimpleKvResponse hlenResponse = client.hlen(hashKey);
        assertTrue(hlenResponse.isOk());
        assertEquals("3", hlenResponse.getFirst());

        
        SimpleKvResponse hgetallResponse = client.hgetall(hashKey);
        assertTrue(hgetallResponse.isOk());
        
        assertEquals(6, hgetallResponse.size());

        
        SimpleKvResponse hdelResponse = client.hdel(hashKey, "age");
        assertTrue(hdelResponse.isOk());
        assertEquals("1", hdelResponse.getFirst());

        
        client.del(hashKey);
    }

    @Test
    void shouldHandleMultipleKeyOperations() throws IOException {
        
        client.set("key1", "value1");
        client.set("key2", "value2");
        client.set("key3", "value3");

        
        SimpleKvResponse delResponse = client.del("key1", "key2", "key3");
        assertTrue(delResponse.isOk());

        
        SimpleKvResponse existsResponse = client.exists("key1", "key2", "key3");
        assertTrue(existsResponse.isOk());
    }

    @Test
    void shouldHandleSpecialCharacters() throws IOException {
        String specialValue = "Value with spaces and special chars: !@#$%^&*()";
        
        client.set("special-key", specialValue);
        SimpleKvResponse response = client.get("special-key");
        
        assertTrue(response.isOk());
        assertEquals(specialValue, response.getFirst());

        client.del("special-key");
    }

    @Test
    void shouldHandleEmptyListOperations() throws IOException {
        String emptyListKey = "empty-list";

        
        SimpleKvResponse llenResponse = client.llen(emptyListKey);
        assertTrue(llenResponse.isOk());
        assertEquals("0", llenResponse.getFirst());

        
        SimpleKvResponse lpopResponse = client.lpop(emptyListKey);
        assertTrue(lpopResponse.isOk());
        assertNull(lpopResponse.getFirst());
    }
}
