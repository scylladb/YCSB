/*
 * Copyright (c) 2026 YCSB contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */
package site.ycsb.db;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit-level test for {@link AlternatorV1Router}.
 *
 * <p>Spins up an in-process HTTP server that fakes the {@code /localnodes} endpoint, points the
 * router at it, and verifies (a) the seeded snapshot resolves a client immediately, (b) the
 * router converges on the discovered set of nodes, and (c) {@code nextClient()} round-robins
 * across them. Sits in the same package as {@link AlternatorV1Router} to access the
 * package-private constructor without reflection or widening visibility.
 */
public class AlternatorV1RouterTest {

  private HttpServer server;
  private final AtomicReference<String> localnodesBody = new AtomicReference<>("[\"127.0.0.1\"]");
  private AlternatorV1Router router;

  @Before
  public void start() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/localnodes", new LocalNodesHandler());
    server.createContext("/", new SwallowHandler());
    server.setExecutor(null);
    server.start();
  }

  @After
  public void stop() {
    if (router != null) {
      router.close();
    }
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  public void seededClientAvailableImmediately() {
    var seedPort = server.getAddress().getPort();
    router = newRouter(seedPort, null, null);
    AmazonDynamoDB c = router.nextClient();
    assertNotNull("router should hand out the seeded client immediately", c);
  }

  @Test
  public void discoversAndRoundRobins() throws Exception {
    localnodesBody.set("[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]");
    var seedPort = server.getAddress().getPort();
    router = newRouter(seedPort, null, null);

    waitForNodeCount(3, 5000);

    Set<AmazonDynamoDB> seen = new HashSet<>();
    for (int i = 0; i < 60; i++) {
      seen.add(router.nextClient());
    }
    assertEquals("expected exactly 3 distinct round-robin clients", 3, seen.size());
  }

  @Test
  public void dropsRemovedNodes() throws Exception {
    localnodesBody.set("[\"10.0.0.1\",\"10.0.0.2\",\"10.0.0.3\"]");
    var seedPort = server.getAddress().getPort();
    router = newRouter(seedPort, null, null);
    waitForNodeCount(3, 5000);

    localnodesBody.set("[\"10.0.0.1\"]");
    waitForNodeCount(1, 5000);

    Set<AmazonDynamoDB> seen = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      seen.add(router.nextClient());
    }
    assertEquals("router should converge to the single live node", 1, seen.size());
  }

  // ---- Helpers --------------------------------------------------------------

  private AlternatorV1Router newRouter(int port, String dc, String rack) {
    return new AlternatorV1Router(
        URI.create("http://127.0.0.1:" + port),
        port,
        "us-east-1",
        dc,
        rack,
        new ClientConfiguration().withMaxConnections(8),
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("test", "test")));
  }

  private void waitForNodeCount(int expected, long timeoutMillis) throws Exception {
    var deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (snapshotSize(router) == expected) {
        return;
      }
      Thread.sleep(100);
    }
    assertEquals("router did not reach expected node count within timeout",
        expected, snapshotSize(router));
  }

  private static int snapshotSize(AlternatorV1Router router) throws Exception {
    var f = AlternatorV1Router.class.getDeclaredField("snapshot");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    var snap = (List<AmazonDynamoDB>) f.get(router);
    return snap == null ? 0 : snap.size();
  }

  private final class LocalNodesHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange ex) throws IOException {
      var body = localnodesBody.get().getBytes(StandardCharsets.UTF_8);
      ex.getResponseHeaders().add("Content-Type", "application/json");
      ex.sendResponseHeaders(200, body.length);
      try (OutputStream os = ex.getResponseBody()) {
        os.write(body);
      }
    }
  }

  private static final class SwallowHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange ex) throws IOException {
      var body = "{}".getBytes(StandardCharsets.UTF_8);
      ex.getResponseHeaders().add("Content-Type", "application/x-amz-json-1.0");
      ex.sendResponseHeaders(200, body.length);
      try (OutputStream os = ex.getResponseBody()) {
        os.write(body);
      }
    }
  }
}
