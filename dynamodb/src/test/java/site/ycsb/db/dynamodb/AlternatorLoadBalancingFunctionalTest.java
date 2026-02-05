/*
 * Copyright (c) 2026 YCSB contributors.
 */
package site.ycsb.db.dynamodb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.DynamoDBClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Functional test that validates client-side load balancing by alternating endpoints.
 * Uses two in-JVM mock Alternator servers sharing a backing store, so operations succeed
 * regardless of which endpoint is chosen. Verifies that requests hit both endpoints.
 */
public class AlternatorLoadBalancingFunctionalTest {

  private static MockAlternatorServer serverA;
  private static MockAlternatorServer serverB;

  @BeforeClass
  public static void startServers() throws Exception {
    serverA = new MockAlternatorServer(18010);
    serverB = new MockAlternatorServer(18011);
    serverA.start();
    serverB.start();
  }

  @AfterClass
  public static void stopServers() {
    if (serverA != null) serverA.stop();
    if (serverB != null) serverB.stop();
  }

  @Test
  public void roundRobinAcrossEndpoints() throws Exception {
    DynamoDBClient client = getDynamoDBClient();
    try {
      String table = "usertable";

      // Insert a bunch of items
      for (int i = 0; i < 10; i++) {
        Map<String, ByteIterator> v = new HashMap<>();
        v.put("f", new StringByteIterator("v" + i));
        assertThat(client.insert(table, "k" + i, v), is(Status.OK));
      }

      // Read a few back
      for (int i = 0; i < 5; i++) {
        Map<String, ByteIterator> out = new HashMap<>();
        assertThat(client.read(table, "k" + i, null, out), is(Status.OK));
        assertThat(out.get("f").toString(), is("v" + i));
      }

      // Verify both endpoints were used
      List<String> seen = CaptureInterceptor.seen;
      boolean usedA = seen.stream().anyMatch(s -> s.endsWith("127.0.0.1:18010") || s.endsWith("localhost:18010"));
      boolean usedB = seen.stream().anyMatch(s -> s.endsWith("127.0.0.1:18011") || s.endsWith("localhost:18011"));
      assertTrue("Expected endpoint A used, seen=" + seen, usedA);
      assertTrue("Expected endpoint B used, seen=" + seen, usedB);
    } finally {
      client.cleanup();
    }
  }

  private static @NotNull DynamoDBClient getDynamoDBClient() throws DBException {
    Properties props = new Properties();
    props.setProperty("dynamodb.endpoint", "http://127.0.0.1:9999"); // ignored by staticNodes
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.accessKey", "test");
    props.setProperty("dynamodb.secretKey", "test");
    props.setProperty("dynamodb.primaryKey", "y_id");
    props.setProperty("dynamodb.alternator.loadbalancing", "true");
    props.setProperty("dynamodb.alternator.staticNodes", "http://127.0.0.1:18010,http://127.0.0.1:18011");
    props.setProperty("dynamodb.executionInterceptors", "site.ycsb.db.dynamodb.AlternatorLoadBalancingFunctionalTest$CaptureInterceptor");

    DynamoDBClient client = new DynamoDBClient();
    client.setProperties(props);
    client.init();
    return client;
  }

  public static final class CaptureInterceptor implements software.amazon.awssdk.core.interceptor.ExecutionInterceptor {
    public static final List<String> seen = new CopyOnWriteArrayList<>();
    @Override public void beforeTransmission(software.amazon.awssdk.core.interceptor.Context.BeforeTransmission ctx,
                                            software.amazon.awssdk.core.interceptor.ExecutionAttributes attrs) {
      try { seen.add(ctx.httpRequest().getUri().getAuthority()); } catch (Throwable ignore) {}
    }
  }

  // ---------------- Mock Alternator ----------------
  private static final class MockAlternatorServer implements HttpHandler {
    private static final Map<String, Map<String, Map<String, String>>> STORE = new ConcurrentHashMap<>();
    private final int port;
    private HttpServer server;

    MockAlternatorServer(int port) { this.port = port; }

    void start() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
      server.createContext("/", this);
      server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
      server.start();
    }
    void stop() { if (server != null) server.stop(0); }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
      byte[] body = readAll(exchange.getRequestBody());
      String json = new String(body, StandardCharsets.UTF_8);
      String response;
      int code = 200;
      try {
        if (target == null) throw new IllegalArgumentException("Missing X-Amz-Target");
        String compact = json.replaceAll("\\s+", "");
        if (target.endsWith("PutItem")) {
          String table = between(compact, "\"TableName\":\"", "\"");
          String key = between(compact, "\"y_id\":{\"S\":\"", "\"");
          String f   = between(compact, "\"f\":{\"S\":\"", "\"");
          assert key != null;
          assert f != null;
          STORE.computeIfAbsent(table, t -> new ConcurrentHashMap<>())
              .put(key, new HashMap<>(Map.of("y_id", key, "f", f)));
          response = "{}";
        } else if (target.endsWith("GetItem")) {
          String table = between(compact, "\"TableName\":\"", "\"");
          String key = between(compact, "\"y_id\":{\"S\":\"", "\"");
          Map<String, String> item = Optional.ofNullable(STORE.get(table)).map(m -> m.get(key)).orElse(null);
          response = (item == null) ? "{}" : toDynamoItemJson(item);
        } else {
          // Minimal stubs for other calls
          response = "{}";
        }
      } catch (Exception e) {
        code = 500;
        response = "{\"error\":\"" + e.getMessage().replace('"','\'') + "\"}";
      }
      byte[] out = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.0");
      exchange.sendResponseHeaders(code, out.length);
      try (OutputStream os = exchange.getResponseBody()) { os.write(out); }
    }

    private static byte[] readAll(InputStream is) throws IOException {
      return is.readAllBytes();
    }

    private static String between(String s, String start, String end) {
      int i = s.indexOf(start);
      if (i < 0) return null;
      i += start.length();
      int j = s.indexOf(end, i);
      if (j < 0) return null;
      return s.substring(i, j);
    }

    private static String toDynamoItemJson(Map<String, String> item) {
      StringBuilder sb = new StringBuilder("{\"Item\":{");
      boolean first = true;
      for (Map.Entry<String, String> e : item.entrySet()) {
        if (!first) sb.append(',');
        sb.append('\"').append(e.getKey()).append("\":{\"S\":\"")
            .append(escape(e.getValue())).append("\"}");
        first = false;
      }
      sb.append("}}\n");
      return sb.toString();
    }

    private static String escape(String s) {
      return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
  }
}
