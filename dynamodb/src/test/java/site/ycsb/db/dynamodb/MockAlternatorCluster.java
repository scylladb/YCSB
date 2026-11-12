/*
 * Copyright (c) 2026 YCSB contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package site.ycsb.db.dynamodb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * In-process Alternator cluster for functional tests of the load-balancing path.
 *
 * <p>Each node is one {@link HttpServer} listener on its own IPv4 loopback address. All
 * listeners share one port, because {@code /localnodes} returns hosts only and the client
 * uses the seed port for every discovered node. The handler identifies the node by the local
 * address that the connection reached. This shows where the client really sent each request,
 * after the Alternator client picked the node for that attempt.
 *
 * <p>Linux routes all of {@code 127.0.0.0/8} to loopback. On macOS, add each extra address
 * first, for example {@code sudo ifconfig lo0 alias 127.0.0.2}.
 *
 * <p>A node that is marked down closes each exchange without a response. The AWS SDK sees a
 * transport failure, which is the nearest in-process match for "connection refused".
 * {@code /localnodes} lists only the nodes that are up, as the Scylla gossiper does.
 */
final class MockAlternatorCluster implements HttpHandler, AutoCloseable {

  private final Map<String, Boolean> nodeUp = new LinkedHashMap<>();
  private final Map<String, Map<String, String>> items = new ConcurrentHashMap<>();
  private final List<Request> requests = new CopyOnWriteArrayList<>();
  private final AtomicInteger localNodesServed = new AtomicInteger();
  private final AtomicInteger errorResponses = new AtomicInteger();
  private final List<HttpServer> servers = new ArrayList<>();
  private ExecutorService executor;

  /** One DynamoDB API request as the mock received it. */
  static final class Request {
    private final String node;
    private final String operation;

    Request(String node, String operation) {
      this.node = node;
      this.operation = operation;
    }

    String node() {
      return node;
    }

    String operation() {
      return operation;
    }

    @Override
    public String toString() {
      return operation + "@" + node;
    }
  }

  MockAlternatorCluster(String... nodes) {
    for (String node : nodes) {
      nodeUp.put(node, true);
    }
  }

  MockAlternatorCluster start() throws IOException {
    executor = Executors.newCachedThreadPool();
    int port = 0;
    for (String node : nodeUp.keySet()) {
      HttpServer server = HttpServer.create(new InetSocketAddress(node, port), 0);
      servers.add(server);
      server.createContext("/", this);
      server.setExecutor(executor);
      server.start();
      port = server.getAddress().getPort();
    }
    return this;
  }

  int port() {
    return servers.get(0).getAddress().getPort();
  }

  @Override
  public void close() {
    try {
      for (HttpServer server : servers) {
        server.stop(0);
      }
    } finally {
      if (executor != null) {
        executor.shutdownNow();
      }
    }
  }

  synchronized void markDown(String node) {
    nodeUp.put(node, false);
  }

  /** Returns every DynamoDB API request received so far, in arrival order. */
  List<Request> requests() {
    return new ArrayList<>(requests);
  }

  /** Returns the requests received after the first {@code from} requests. */
  List<Request> requestsSince(int from) {
    return new ArrayList<>(requests.subList(from, requests.size()));
  }

  int requestCount() {
    return requests.size();
  }

  int localNodesServed() {
    return localNodesServed.get();
  }

  /** Counts responses with a status of 400 or higher from nodes that are up. */
  int errorResponses() {
    return errorResponses.get();
  }

  static void await(BooleanSupplier condition, long timeoutMs, Supplier<String> message)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new TimeoutException(message.get());
      }
      Thread.sleep(20);
    }
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String node = exchange.getLocalAddress().getAddress().getHostAddress();
    String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
    String operation = target == null ? null : target.substring(target.indexOf('.') + 1);
    if (operation != null) {
      requests.add(new Request(node, operation));
    }

    if (!isUp(node)) {
      exchange.close();
      return;
    }

    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)
        .replaceAll("\\s+", "");
    int code = 200;
    String contentType = "application/x-amz-json-1.0";
    String response;
    if (exchange.getRequestURI().getPath().endsWith("/localnodes")) {
      response = liveNodesJson();
      contentType = "application/json";
      localNodesServed.incrementAndGet();
    } else if (operation == null) {
      code = 400;
      response = "{\"error\":\"missing X-Amz-Target\"}";
    } else {
      response = dynamoResponse(operation, body);
    }

    if (code >= 400) {
      errorResponses.incrementAndGet();
    }
    byte[] out = response.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", contentType);
    exchange.sendResponseHeaders(code, out.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(out);
    }
  }

  private synchronized boolean isUp(String node) {
    return Boolean.TRUE.equals(nodeUp.get(node));
  }

  private synchronized String liveNodesJson() {
    StringJoiner list = new StringJoiner(",", "[", "]");
    nodeUp.forEach((node, up) -> {
      if (up) {
        list.add("\"" + node + "\"");
      }
    });
    return list.toString();
  }

  private String dynamoResponse(String operation, String body) {
    String key = between(body, "\"y_id\":{\"S\":\"", "\"");
    switch (operation) {
    case "PutItem":
      if (key != null) {
        Map<String, String> item = new HashMap<>();
        item.put("y_id", key);
        item.put("f", String.valueOf(between(body, "\"f\":{\"S\":\"", "\"")));
        items.put(key, item);
      }
      return "{}";
    case "UpdateItem":
      if (key != null) {
        items.computeIfAbsent(key, k -> new HashMap<>(Map.of("y_id", k)));
      }
      return "{}";
    case "GetItem":
      Map<String, String> item = key == null ? null : items.get(key);
      return item == null ? "{}" : itemJson(item);
    default:
      return "{}";
    }
  }

  private static String between(String s, String start, String end) {
    int i = s.indexOf(start);
    if (i < 0) {
      return null;
    }
    i += start.length();
    int j = s.indexOf(end, i);
    return j < 0 ? null : s.substring(i, j);
  }

  private static String itemJson(Map<String, String> item) {
    StringJoiner attributes = new StringJoiner(",", "{\"Item\":{", "}}");
    item.forEach((name, value) -> attributes.add(
        "\"" + name + "\":{\"S\":\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}"));
    return attributes.toString();
  }
}
