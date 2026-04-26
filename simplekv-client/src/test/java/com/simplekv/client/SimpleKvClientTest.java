package com.simplekv.client;

import com.simplekv.api.store.KeyValueStore;
import com.simplekv.app.cli.SimpleKvCli;
import com.simplekv.app.config.SimpleKvConfiguration;
import com.simplekv.app.config.SimpleKvServerProperties;
import com.simplekv.app.config.SimpleKvStorageProperties;
import com.simplekv.app.diag.DiagnosticService;
import com.simplekv.app.server.SimpleKvServerBanner;
import com.simplekv.app.server.SimpleKvSkspServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimpleKvClientTest {
    private static final String HOST = "127.0.0.1";

    private SimpleKvClient client;
    private SimpleKvSkspServer server;
    private KeyValueStore store;
    private Thread serverThread;
    private Path dataDir;
    private int serverPort;
    private final AtomicReference<Throwable> serverFailure = new AtomicReference<Throwable>();

    @BeforeAll
    void startEmbeddedServer() throws Exception {
        dataDir = Files.createTempDirectory("simplekv-client-it-");

        SimpleKvStorageProperties storageProperties = new SimpleKvStorageProperties();
        storageProperties.setDataDir(dataDir);
        SimpleKvServerProperties serverProperties = new SimpleKvServerProperties();
        serverProperties.setHost(HOST);
        serverProperties.setPort(0);
        serverProperties.setBannerEnabled(false);

        SimpleKvConfiguration configuration = new SimpleKvConfiguration();
        store = configuration.keyValueStore(configuration.storageOptions(storageProperties));
        DiagnosticService diagnosticService = new DiagnosticService(storageProperties);
        SimpleKvCli cli = new SimpleKvCli(store, diagnosticService, configuration.objectMapper(), storageProperties);
        SimpleKvServerBanner banner = new SimpleKvServerBanner(serverProperties, storageProperties);
        server = new SimpleKvSkspServer(cli, store, serverProperties, banner);

        serverThread = Thread.ofVirtual()
                .name("simplekv-client-test-server")
                .start(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            server.start();
                        }
                        catch (Throwable ex) {
                            serverFailure.compareAndSet(null, ex);
                        }
                    }
                });

        boolean started = server.awaitStarted(10, TimeUnit.SECONDS);
        if (!started) {
            throw new IllegalStateException("Timed out waiting for embedded SimpleKV server startup.");
        }
        Throwable startupError = serverFailure.get();
        if (startupError != null) {
            throw new IllegalStateException("Embedded SimpleKV server failed to start.", startupError);
        }
        serverPort = server.getBoundPort();
        if (serverPort <= 0) {
            throw new IllegalStateException("Embedded SimpleKV server did not bind to a valid port.");
        }
    }

    @BeforeEach
    void setUp() throws IOException {
        client = new SimpleKvClient(HOST, serverPort);
        client.connect();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @AfterAll
    void stopEmbeddedServer() throws Exception {
        IOException closeError = null;
        if (server != null) {
            try {
                server.close();
            }
            catch (IOException ex) {
                closeError = ex;
            }
        }
        if (serverThread != null) {
            serverThread.join(TimeUnit.SECONDS.toMillis(5));
        }
        if (store != null) {
            try {
                store.close();
            }
            catch (IOException ex) {
                if (closeError == null) {
                    closeError = ex;
                }
            }
        }
        if (dataDir != null) {
            deleteRecursively(dataDir);
        }
        Throwable runtimeFailure = serverFailure.get();
        if (runtimeFailure != null) {
            throw new IllegalStateException("Embedded SimpleKV server failed during test execution.", runtimeFailure);
        }
        if (closeError != null) {
            throw closeError;
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try {
            try (var paths = Files.walk(root)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    }
                    catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                });
            }
        }
        catch (UncheckedIOException ex) {
            throw ex.getCause();
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
