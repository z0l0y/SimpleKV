package com.simplekv.client.example;

import com.simplekv.client.SimpleKvClient;
import com.simplekv.client.SimpleKvResponse;
import java.io.IOException;

public class ExampleApp {
  public static void main(String[] args) throws IOException {
  System.out.println("=== SimpleKV Client Example ===\n");

  try (SimpleKvClient client = new SimpleKvClient("127.0.0.1", 7379)) {
    System.out.println("Connecting to SimpleKV server at 127.0.0.1:7379...");
    client.connect();
    System.out.println("Connected!\n");

    System.out.println("--- String Operations ---");
    example1StringOps(client);

    System.out.println("\n--- List Operations ---");
    example2ListOps(client);

    System.out.println("\n--- Set Operations ---");
    example3SetOps(client);

    System.out.println("\n--- Hash Operations ---");
    example4HashOps(client);
  } catch (IOException e) {
    System.err.println("Error: " + e.getMessage());
    e.printStackTrace();
  }
  }

  private static void example1StringOps(SimpleKvClient client) throws IOException {
  client.set("user:1:name", "Alice");
  client.set("user:1:email", "alice@example.com");
  client.set("user:1:age", "30");

  SimpleKvResponse nameResponse = client.get("user:1:name");
  SimpleKvResponse emailResponse = client.get("user:1:email");
  SimpleKvResponse ageResponse = client.get("user:1:age");

  System.out.println("User 1 Information:");
  System.out.println("  Name: " + nameResponse.getFirst());
  System.out.println("  Email: " + emailResponse.getFirst());
  System.out.println("  Age: " + ageResponse.getFirst());

  SimpleKvResponse existsResponse = client.exists(
    "user:1:name",
    "user:1:email",
    "user:1:phone");
  System.out.println("Keys found: " + existsResponse.getPayload());

  client.del("user:1:name", "user:1:email", "user:1:age");
  }

  private static void example2ListOps(SimpleKvClient client) throws IOException {
  System.out.println("Creating task queue...");
  client.lpush("tasks", "task-3", "task-2", "task-1");
  client.rpush("tasks", "task-4", "task-5");

  SimpleKvResponse lrangeResponse = client.lrange("tasks", 0, -1);
  System.out.println("All tasks: " + lrangeResponse.getPayload());

  SimpleKvResponse llenResponse = client.llen("tasks");
  System.out.println("Queue length: " + llenResponse.getFirst());

  SimpleKvResponse task1 = client.lpop("tasks");
  SimpleKvResponse task2 = client.lpop("tasks");
  System.out.println("Processed: " + task1.getFirst());
  System.out.println("Processed: " + task2.getFirst());

  client.del("tasks");
  }

  private static void example3SetOps(SimpleKvClient client) throws IOException {
  System.out.println("Adding user interests...");
  client.sadd("user:1:interests", "java", "python", "go", "rust");
  client.sadd("user:2:interests", "java", "kotlin", "scala", "clojure");

  SimpleKvResponse user1Interests = client.smembers("user:1:interests");
  System.out.println("User 1 interests: " + user1Interests.getPayload());

  SimpleKvResponse hasJava = client.sismember("user:1:interests", "java");
  System.out.println("User 1 likes Java: " + ("1".equals(hasJava.getFirst()) ? "Yes" : "No"));

  SimpleKvResponse cardResponse = client.scard("user:1:interests");
  System.out.println("User 1 has " + cardResponse.getFirst() + " interests");

  client.del("user:1:interests", "user:2:interests");
  }

  private static void example4HashOps(SimpleKvClient client) throws IOException {
  System.out.println("Storing product information...");
  client.hset(
    "product:1",
    "name", "Laptop",
    "brand", "Dell",
    "price", "999.99",
    "stock", "50");

  SimpleKvResponse name = client.hget("product:1", "name");
  SimpleKvResponse price = client.hget("product:1", "price");
  System.out.println("Product: " + name.getFirst() + " - $" + price.getFirst());

  SimpleKvResponse allDetails = client.hgetall("product:1");
  System.out.println("All details: " + allDetails.getPayload());

  SimpleKvResponse hasReview = client.hexists("product:1", "review");
  System.out.println("Has review: " + ("1".equals(hasReview.getFirst()) ? "Yes" : "No"));

  SimpleKvResponse fieldCount = client.hlen("product:1");
  System.out.println("Number of fields: " + fieldCount.getFirst());

  client.hset("product:1", "stock", "49");
  SimpleKvResponse updatedStock = client.hget("product:1", "stock");
  System.out.println("Updated stock: " + updatedStock.getFirst());

  client.del("product:1");
  }
}
