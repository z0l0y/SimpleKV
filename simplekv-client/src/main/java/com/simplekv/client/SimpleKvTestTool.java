package com.simplekv.client;

import java.io.IOException;

public class SimpleKvTestTool {
  public static void main(String[] args) throws IOException {
    String host = "127.0.0.1";
    int port = 7379;

    if (args.length > 0) {
    host = args[0];
    }
    if (args.length > 1) {
    port = Integer.parseInt(args[1]);
    }

    System.out.println("SimpleKV Client Test Tool");
    System.out.println("Connecting to " + host + ":" + port + "...");

    try (SimpleKvClient client = new SimpleKvClient(host, port)) {
    client.connect();
    System.out.println("Connected successfully!\n");
    runTests(client);
    } catch (IOException e) {
    System.err.println("Failed to connect: " + e.getMessage());
    e.printStackTrace();
    }
  }

  private static void runTests(SimpleKvClient client) throws IOException {
  System.out.println("=== Basic Commands ===");
  testPing(client);
  testEcho(client);

  System.out.println("\n=== String Operations ===");
  testStringOperations(client);

  System.out.println("\n=== List Operations ===");
  testListOperations(client);

  System.out.println("\n=== Set Operations ===");
  testSetOperations(client);

  System.out.println("\n=== Hash Operations ===");
  testHashOperations(client);

  System.out.println("\n=== All tests completed ===");
  }

  private static void testPing(SimpleKvClient client) throws IOException {
  System.out.println("Testing PING...");
  SimpleKvResponse response = client.ping();
  printResponse("PING", response);
  }

  private static void testEcho(SimpleKvClient client) throws IOException {
  System.out.println("Testing ECHO...");
  SimpleKvResponse response = client.echo("Hello SimpleKV!");
  printResponse("ECHO \"Hello SimpleKV!\"", response);
  }

  private static void testStringOperations(SimpleKvClient client) throws IOException {
  System.out.println("Testing SET...");
  SimpleKvResponse setResponse = client.set("username", "alice");
  printResponse("SET username alice", setResponse);

  System.out.println("Testing GET...");
  SimpleKvResponse getResponse = client.get("username");
  printResponse("GET username", getResponse);

  System.out.println("Testing DEL...");
  SimpleKvResponse delResponse = client.del("username");
  printResponse("DEL username", delResponse);

  System.out.println("Testing SET with spaces...");
  SimpleKvResponse setResponse2 = client.set("greeting", "Hello World");
  printResponse("SET greeting \"Hello World\"", setResponse2);

  System.out.println("Testing GET greeting...");
  SimpleKvResponse getResponse2 = client.get("greeting");
  printResponse("GET greeting", getResponse2);

  client.del("greeting");
  }

  private static void testListOperations(SimpleKvClient client) throws IOException {
  String listKey = "mylist";

  System.out.println("Testing LPUSH...");
  SimpleKvResponse lpushResponse = client.lpush(listKey, "3", "2", "1");
  printResponse("LPUSH " + listKey + " 3 2 1", lpushResponse);

  System.out.println("Testing LLEN...");
  SimpleKvResponse llenResponse = client.llen(listKey);
  printResponse("LLEN " + listKey, llenResponse);

  System.out.println("Testing LRANGE...");
  SimpleKvResponse lrangeResponse = client.lrange(listKey, 0, -1);
  printResponse("LRANGE " + listKey + " 0 -1", lrangeResponse);

  System.out.println("Testing RPUSH...");
  SimpleKvResponse rpushResponse = client.rpush(listKey, "4", "5");
  printResponse("RPUSH " + listKey + " 4 5", rpushResponse);

  System.out.println("Testing LPOP...");
  SimpleKvResponse lpopResponse = client.lpop(listKey);
  printResponse("LPOP " + listKey, lpopResponse);

  System.out.println("Testing RPOP...");
  SimpleKvResponse rpopResponse = client.rpop(listKey);
  printResponse("RPOP " + listKey, rpopResponse);

  client.del(listKey);
  }

  private static void testSetOperations(SimpleKvClient client) throws IOException {
  String setKey = "myset";

  System.out.println("Testing SADD...");
  SimpleKvResponse saddResponse = client.sadd(setKey, "red", "green", "blue", "red");
  printResponse("SADD " + setKey + " red green blue red", saddResponse);

  System.out.println("Testing SCARD...");
  SimpleKvResponse scardResponse = client.scard(setKey);
  printResponse("SCARD " + setKey, scardResponse);

  System.out.println("Testing SISMEMBER...");
  SimpleKvResponse sismemberResponse = client.sismember(setKey, "red");
  printResponse("SISMEMBER " + setKey + " red", sismemberResponse);

  System.out.println("Testing SMEMBERS...");
  SimpleKvResponse smembersResponse = client.smembers(setKey);
  printResponse("SMEMBERS " + setKey, smembersResponse);

  System.out.println("Testing SREM...");
  SimpleKvResponse sremResponse = client.srem(setKey, "red", "yellow");
  printResponse("SREM " + setKey + " red yellow", sremResponse);

  client.del(setKey);
  }

  private static void testHashOperations(SimpleKvClient client) throws IOException {
  String hashKey = "myhash";

  System.out.println("Testing HSET...");
  SimpleKvResponse hsetResponse = client.hset(hashKey, "field1", "value1", "field2", "value2");
  printResponse("HSET " + hashKey + " field1 value1 field2 value2", hsetResponse);

  System.out.println("Testing HGET...");
  SimpleKvResponse hgetResponse = client.hget(hashKey, "field1");
  printResponse("HGET " + hashKey + " field1", hgetResponse);

  System.out.println("Testing HEXISTS...");
  SimpleKvResponse hexistsResponse = client.hexists(hashKey, "field1");
  printResponse("HEXISTS " + hashKey + " field1", hexistsResponse);

  System.out.println("Testing HLEN...");
  SimpleKvResponse hlenResponse = client.hlen(hashKey);
  printResponse("HLEN " + hashKey, hlenResponse);

  System.out.println("Testing HGETALL...");
  SimpleKvResponse hgetallResponse = client.hgetall(hashKey);
  printResponse("HGETALL " + hashKey, hgetallResponse);

  System.out.println("Testing HDEL...");
  SimpleKvResponse hdelResponse = client.hdel(hashKey, "field1");
  printResponse("HDEL " + hashKey + " field1", hdelResponse);

  client.del(hashKey);
  }

  private static void printResponse(String command, SimpleKvResponse response) {
  System.out.println("  Command: " + command);
  System.out.println("  Status: " + response.getStatus() + " " + response.getExitCode());
  if (!response.isEmpty()) {
    System.out.println("  Payload (" + response.size() + " lines):");
    for (String line : response.getPayload()) {
    System.out.println("    " + line);
    }
  } else {
    System.out.println("  Payload: (empty)");
  }
  }
}
