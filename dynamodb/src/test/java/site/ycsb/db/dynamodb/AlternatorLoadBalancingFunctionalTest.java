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
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class AlternatorLoadBalancingFunctionalTest {

  private static MockAlternatorServer serverA;
  private static MockAlternatorServer serverB;
  private static MockDiscoveryServer discoveryServer;

  @BeforeClass
  public static void startServers() throws Exception {
    // Since AlternatorConfig will use the same port for all nodes,
    // we only need the discovery server that can handle all requests
    discoveryServer = new MockDiscoveryServer(9999);
    discoveryServer.start();
  }

  @AfterClass
  public static void stopServers() {
    if (discoveryServer != null) discoveryServer.stop();
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
      boolean usedLocalhost = seen.stream().anyMatch(s -> s.contains("localhost:9999"));
      boolean used127001 = seen.stream().anyMatch(s -> s.contains("127.0.0.1:9999"));
      assertTrue("Expected localhost:9999 endpoint used, seen=" + seen, usedLocalhost);
      assertTrue("Expected 127.0.0.1:9999 endpoint used, seen=" + seen, used127001);
      System.out.println("✅ Load balancing working! Endpoints seen: " + seen);
    } finally {
      client.cleanup();
    }
  }

  private static @NotNull DynamoDBClient getDynamoDBClient() throws DBException {
    Properties props = new Properties();
    props.setProperty("dynamodb.endpoint", "http://127.0.0.1:9999"); // discovery endpoint
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.accessKey", "test");
    props.setProperty("dynamodb.secretKey", "test");
    props.setProperty("dynamodb.primaryKey", "y_id");
    props.setProperty("dynamodb.alternator.loadbalancing", "true");
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
    static final Map<String, Map<String, Map<String, String>>> STORE = new ConcurrentHashMap<>();
    private final int port;
    private final String host;
    private HttpServer server;

    MockAlternatorServer(int port, String host) {
      this.port = port;
      this.host = host;
    }

    void start() throws IOException {
      server = HttpServer.create(new InetSocketAddress(host, port), 0);
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

  // ---------------- Mock Discovery Server ----------------
  private static final class MockDiscoveryServer implements HttpHandler {
    private final int port;
    private HttpServer server;
    private final AtomicInteger requestCounter = new AtomicInteger(0);

    MockDiscoveryServer(int port) { this.port = port; }


    void start() throws IOException {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
      server.createContext("/", this);
      server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
      server.start();
      System.out.println("Discovery server started on port " + port);
    }

    void stop() {
      if (server != null) {
        server.stop(0);
        System.out.println("Discovery server stopped");
      }
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String path = exchange.getRequestURI().getPath();
      String query = exchange.getRequestURI().getQuery();
      String method = exchange.getRequestMethod();

      System.out.println("Discovery server received: " + method + " " + path + (query != null ? "?" + query : ""));

      String response;
      int code = 200;
      String contentType = "application/json";

      try {
        if ("GET".equals(method)) {
          response = switch (path) {
            // Core endpoints for node discovery
            case "/storage_service/nodes/live" ->
              "[\"127.0.0.1:18010\", \"127.0.0.1:18011\"]";
            case "/storage_service/host_id" ->
              "{\"127.0.0.1:18010\": \"11111111-1111-1111-1111-111111111111\", \"127.0.0.1:18011\": \"22222222-2222-2222-2222-222222222222\"}";
            case "/storage_service/host_id/local" ->
              "\"11111111-1111-1111-1111-111111111111\"";
            case "/snitch/datacenter" ->
              "\"datacenter1\"";
            case "/snitch/rack" ->
              "\"rack1\"";
            case "/snitch/datacenter/127.0.0.1:18010", "/snitch/datacenter/127.0.0.1%3A18010" ->
              "\"datacenter1\"";
            case "/snitch/datacenter/127.0.0.1:18011", "/snitch/datacenter/127.0.0.1%3A18011" ->
              "\"datacenter1\"";
            case "/snitch/rack/127.0.0.1:18010", "/snitch/rack/127.0.0.1%3A18010" ->
              "\"rack1\"";
            case "/snitch/rack/127.0.0.1:18011", "/snitch/rack/127.0.0.1%3A18011" ->
              "\"rack1\"";

            // Token-related endpoints
            case "/storage_service/tokens" ->
              "[\"1\", \"2\", \"3\"]";
            case "/storage_service/tokens/127.0.0.1:18010", "/storage_service/tokens/127.0.0.1%3A18010" ->
              "[\"1\"]";
            case "/storage_service/tokens/127.0.0.1:18011", "/storage_service/tokens/127.0.0.1%3A18011" ->
              "[\"2\"]";

            // Gossiper endpoints
            case "/gossiper/endpoint/live" ->
              "[\"127.0.0.1:18010\", \"127.0.0.1:18011\"]";
            case "/gossiper/endpoint/down" ->
              "[]";

            // Failure detector endpoints
            case "/failure_detector/endpoints" ->
              "[\"127.0.0.1:18010\", \"127.0.0.1:18011\"]";
            case "/failure_detector/simple_states" ->
              "{\"127.0.0.1:18010\": \"UP\", \"127.0.0.1:18011\": \"UP\"}";

            // Local node endpoints
            case "/storage_service/nodes/local" ->
              "\"127.0.0.1:9999\"";
            case "/gossiper/endpoint/local" ->
              "\"127.0.0.1:9999\"";

            // Schema and keyspace endpoints (for completeness)
            case "/storage_service/keyspaces" ->
              "[\"system\", \"ycsb\"]";
            case "/column_family" ->
              "[]";

            // Alternator-specific endpoints - just return hostnames, client will use seed port
            case "/localnodes" ->
              "[\"localhost\", \"127.0.0.1\"]";
            case "/v1/localnodes" ->
              "[\"localhost\", \"127.0.0.1\"]";
            case "/v2/localnodes" ->
              "[\"localhost\", \"127.0.0.1\"]";

            default -> {
              // Handle any DynamoDB requests by forwarding to one of the Alternator servers
              String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
              if (target != null) {
                yield handleDynamoDbRequest(exchange, target);
              } else {
                System.out.println("Unhandled discovery endpoint: " + path);
                yield "{}";
              }
            }
          };
        } else {
          // Handle DynamoDB requests
          String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
          if (target != null) {
            response = handleDynamoDbRequest(exchange, target);
            contentType = "application/x-amz-json-1.0";
          } else {
            response = "{}";
          }
        }
      } catch (Exception e) {
        code = 500;
        response = "{\"error\":\"" + e.getMessage().replace('"','\'') + "\"}";
        e.printStackTrace();
      }

      byte[] out = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", contentType);
      exchange.sendResponseHeaders(code, out.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(out);
      }
    }

    private String handleDynamoDbRequest(HttpExchange exchange, String target) throws IOException {
      byte[] body = readAll(exchange.getRequestBody());
      String json = new String(body, StandardCharsets.UTF_8);

      if (target == null) throw new IllegalArgumentException("Missing X-Amz-Target");
      String compact = json.replaceAll("\\s+", "");
      if (target.endsWith("PutItem")) {
        String table = between(compact, "\"TableName\":\"", "\"");
        String key = between(compact, "\"y_id\":{\"S\":\"", "\"");
        String f   = between(compact, "\"f\":{\"S\":\"", "\"");
        if (key != null && f != null) {
          MockAlternatorServer.STORE.computeIfAbsent(table, t -> new ConcurrentHashMap<>())
              .put(key, new HashMap<>(Map.of("y_id", key, "f", f)));
        }
        return "{}";
      } else if (target.endsWith("GetItem")) {
        String table = between(compact, "\"TableName\":\"", "\"");
        String key = between(compact, "\"y_id\":{\"S\":\"", "\"");
        Map<String, String> item = Optional.ofNullable(MockAlternatorServer.STORE.get(table))
            .map(m -> m.get(key)).orElse(null);
        return (item == null) ? "{}" : toDynamoItemJson(item);
      } else {
        // Minimal stubs for other calls
        return "{}";
      }
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
