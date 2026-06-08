/*
 * Copyright (c) 2026 YCSB contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package site.ycsb.db.dynamodb;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Reproducer for the production failure where YCSB UPDATE operations exited with
 * {@code CLIENT_ERROR (connection refused)} after the Alternator seed node went down
 * during a rolling upgrade. With native load balancing enabled the YCSB binding configures
 * {@link com.scylladb.alternator.AlternatorDynamoDbAsyncClient} with a single seed URI; the
 * load balancer polls {@code /localnodes} on that seed to learn about the rest of the
 * cluster and round-robins requests across the discovered nodes. When the seed itself is
 * the dead node, two things can go wrong simultaneously:
 *
 * <ul>
 *   <li>requests that round-robin onto the dead seed fail with connection refused, and</li>
 *   <li>{@code /localnodes} refreshes against the seed fail, so the live-node list can no
 *       longer be updated.</li>
 * </ul>
 *
 * <p>The test boots a single {@link HttpServer} that hosts two logical Alternator endpoints
 * — distinguished by request {@code Host} header — both physically served by the same
 * loopback listener. The hostnames {@code localhost} and {@code 127.0.0.1} both resolve to
 * the loopback address without any custom DNS resolver, which is the same trick used by
 * {@link AlternatorLoadBalancingFunctionalTest}. The "seed" hostname can be flipped to a
 * down state at any time; in that state, the handler silently closes the HTTP exchange
 * without writing a response, which the AWS SDK observes as a transport-level failure
 * equivalent to a refused/reset connection.
 */
public class AlternatorSeedFailoverFunctionalTest {

  private static final String SEED_HOST = "localhost";
  private static final String PEER_HOST = "127.0.0.1";
  private static final String TABLE = "usertable";

  private DualHostMockAlternator server;
  private int port;

  @Before
  public void start() throws Exception {
    server = new DualHostMockAlternator();
    server.start();
    port = server.port();
    CaptureInterceptor.seen.clear();
  }

  @After
  public void stop() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void seedDeathDoesNotKillAllTraffic() throws Exception {
    DynamoDBClient client = newClient();
    try {
      // Warm-up: drive both endpoints so the Alternator load balancer has learned the live
      // node list and is round-robining across both hostnames.
      for (int i = 0; i < 20; i++) {
        Map<String, ByteIterator> v = new HashMap<>();
        v.put("f", new StringByteIterator("warm-" + i));
        assertThat("warm-up insert " + i, client.insert(TABLE, "k" + i, v), is(Status.OK));
      }
      assertTrue("expected seed host to receive traffic before kill, seen=" + CaptureInterceptor.seen,
          CaptureInterceptor.seen.stream().anyMatch(s -> s.startsWith(SEED_HOST + ":")));
      assertTrue("expected peer host to receive traffic before kill, seen=" + CaptureInterceptor.seen,
          CaptureInterceptor.seen.stream().anyMatch(s -> s.startsWith(PEER_HOST + ":")));

      // Kill the seed — mirrors node 0-2 going down at 19:46:36 in the failure timeline.
      // The peer remains healthy. From here on, any request the LB picks for the seed will
      // fail with a connection-style error; any request picked for the peer must still
      // succeed. A resilient client should converge on the peer and keep making forward
      // progress instead of returning CLIENT_ERROR for every request that lands on the seed.
      server.markDown(SEED_HOST);

      // Allow the background refresh thread (active interval is 1s by default) at least one
      // pass to observe the dead seed and prune it from the live node list. The production
      // failure burned through tens of thousands of requests over multiple seconds, so a
      // resilient LB has ample time to converge; we wait just enough for that convergence
      // to happen here.
      Thread.sleep(2500);

      int total = 200;
      int peerOnlyHits = 0;
      int seedAttemptFailures = 0;
      int peerFailures = 0;
      int statusOk = 0;
      int statusErr = 0;

      int beforePost = CaptureInterceptor.seen.size();

      for (int i = 0; i < total; i++) {
        Map<String, ByteIterator> v = new HashMap<>();
        v.put("f", new StringByteIterator("post-kill-" + i));
        Status s = client.update(TABLE, "k" + (i % 20), v);
        if (s == Status.OK) {
          statusOk++;
        } else {
          statusErr++;
        }
      }

      List<String> postKill = new CopyOnWriteArrayList<>(
          CaptureInterceptor.seen.subList(beforePost, CaptureInterceptor.seen.size()));

      for (String authority : postKill) {
        if (authority.startsWith(PEER_HOST + ":")) {
          peerOnlyHits++;
        }
      }
      seedAttemptFailures = (int) postKill.stream().filter(a -> a.startsWith(SEED_HOST + ":")).count();
      peerFailures = server.peerFailures();

      System.out.println("post-kill summary:");
      System.out.println("  total update calls       = " + total);
      System.out.println("  status OK                = " + statusOk);
      System.out.println("  status non-OK            = " + statusErr);
      System.out.println("  requests dispatched to peer  = " + peerOnlyHits);
      System.out.println("  requests dispatched to seed  = " + seedAttemptFailures);
      System.out.println("  failures observed on peer    = " + peerFailures);

      // The peer is fully healthy and the load balancer has known about it since startup,
      // so once the seed dies the LB should evict it and converge on the peer — every
      // update should succeed. The bug under reproduction is that the LB keeps the dead
      // seed in its rotation, so roughly half the post-kill requests are still dispatched
      // to the seed and fail with "connection refused". That is exactly the failure mode
      // observed in production (62,624 UPDATEs returning CLIENT_ERROR while only one node
      // was actually down).
      //
      // We assert zero failures: when this test fails, the printed summary above quantifies
      // how badly the LB is failing to evict the dead seed. When the underlying behavior is
      // fixed — by either the binding or the load-balancing library — this assertion will
      // start passing.
      assertThat("peer must never have observed errors during the test; if this fails the "
              + "mock is broken, not the LB", peerFailures, is(0));
      assertThat("expected all " + total + " post-kill updates to succeed via the healthy "
              + "peer (the LB should evict the dead seed). " + statusErr + " failed — "
              + "reproduces the production failure where YCSB exited 1 after seeing "
              + "connection-refused errors on the Alternator endpoint of the only down node.",
          statusErr, is(0));
    } finally {
      client.cleanup();
    }
  }

  // ---------------- Client wiring ----------------

  private DynamoDBClient newClient() throws DBException {
    Properties props = new Properties();
    // Seed points at "localhost" — that hostname is the one that will be killed mid-test.
    props.setProperty("dynamodb.endpoint", "http://" + SEED_HOST + ":" + port);
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.accessKey", "test");
    props.setProperty("dynamodb.secretKey", "test");
    props.setProperty("dynamodb.primaryKey", "y_id");
    props.setProperty("dynamodb.alternator.loadbalancing", "true");
    props.setProperty("dynamodb.executionInterceptors",
        "site.ycsb.db.dynamodb.AlternatorSeedFailoverFunctionalTest$CaptureInterceptor");

    DynamoDBClient client = new DynamoDBClient();
    client.setProperties(props);
    client.init();
    return client;
  }

  public static final class CaptureInterceptor
      implements software.amazon.awssdk.core.interceptor.ExecutionInterceptor {
    public static final List<String> seen = new CopyOnWriteArrayList<>();

    @Override
    public void beforeTransmission(software.amazon.awssdk.core.interceptor.Context.BeforeTransmission ctx,
                                   software.amazon.awssdk.core.interceptor.ExecutionAttributes attrs) {
      try {
        seen.add(ctx.httpRequest().getUri().getAuthority());
      } catch (Throwable ignore) {
        // intentionally swallowed: interceptor must never break the request pipeline
      }
    }
  }

  // ---------------- Mock Alternator with per-host kill switch ----------------

  /**
   * Single HTTP listener bound to 127.0.0.1 that distinguishes "logical nodes" by the
   * incoming {@code Host} header. Each logical node has an independent up/down flag; while
   * down, the handler closes the exchange without sending a response, which the AWS SDK
   * surfaces as a transport-level failure — the closest in-process equivalent of the
   * "connection refused to 10.142.0.148:8080" the failing run observed.
   */
  private static final class DualHostMockAlternator implements HttpHandler {
    final Map<String, Map<String, Map<String, String>>> store = new ConcurrentHashMap<>();
    private final Map<String, Boolean> hostUp = new ConcurrentHashMap<>();
    private final AtomicInteger peerFailureCount = new AtomicInteger();
    private HttpServer server;

    DualHostMockAlternator() {
      hostUp.put(SEED_HOST, true);
      hostUp.put(PEER_HOST, true);
    }

    void start() throws IOException {
      // Bind to 127.0.0.1 only — both SEED_HOST ("localhost") and PEER_HOST ("127.0.0.1")
      // resolve to this address, so the single listener serves both logical hostnames and
      // the Host header is the only thing telling them apart.
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/", this);
      server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
      server.start();
    }

    int port() {
      return server.getAddress().getPort();
    }

    void stop() {
      if (server != null) {
        server.stop(0);
      }
    }

    void markDown(String host) {
      hostUp.put(host, false);
    }

    int peerFailures() {
      return peerFailureCount.get();
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String hostHeader = exchange.getRequestHeaders().getFirst("Host");
      String logicalHost = hostHeader == null ? "" : hostHeader.split(":", 2)[0];

      if (Boolean.FALSE.equals(hostUp.get(logicalHost))) {
        // Slam the connection shut without a response. The AWS SDK sees this as an IO
        // failure (Connection reset / closed prematurely), the same failure category as the
        // production "connection refused" from a stopped Alternator listener.
        exchange.close();
        return;
      }

      String path = exchange.getRequestURI().getPath();
      String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
      byte[] body = readAll(exchange.getRequestBody());
      String json = new String(body, StandardCharsets.UTF_8);

      int code = 200;
      String response;
      String contentType = "application/x-amz-json-1.0";

      try {
        if ("/localnodes".equals(path) || "/v1/localnodes".equals(path) || "/v2/localnodes".equals(path)) {
          // A real Scylla peer's gossiper drops dead nodes from /localnodes within a few
          // seconds of the down event, so the response we serve only contains hosts that
          // are currently up.
          StringBuilder list = new StringBuilder("[");
          boolean first = true;
          for (Map.Entry<String, Boolean> e : hostUp.entrySet()) {
            if (!Boolean.TRUE.equals(e.getValue())) {
              continue;
            }
            if (!first) {
              list.append(", ");
            }
            list.append('"').append(e.getKey()).append('"');
            first = false;
          }
          list.append("]");
          response = list.toString();
          contentType = "application/json";
        } else if (target != null && target.endsWith("PutItem")) {
          String compact = json.replaceAll("\\s+", "");
          String table = between(compact, "\"TableName\":\"", "\"");
          String key = between(compact, "\"y_id\":{\"S\":\"", "\"");
          String f = between(compact, "\"f\":{\"S\":\"", "\"");
          if (key != null) {
            store.computeIfAbsent(table == null ? TABLE : table, t -> new ConcurrentHashMap<>())
                .put(key, new HashMap<>(Map.of("y_id", key, "f", f == null ? "" : f)));
          }
          response = "{}";
        } else if (target != null && target.endsWith("GetItem")) {
          String compact = json.replaceAll("\\s+", "");
          String table = between(compact, "\"TableName\":\"", "\"");
          String key = between(compact, "\"y_id\":{\"S\":\"", "\"");
          Map<String, String> item = Optional.ofNullable(store.get(table == null ? TABLE : table))
              .map(m -> m.get(key)).orElse(null);
          response = (item == null) ? "{}" : toDynamoItemJson(item);
        } else if (target != null && target.endsWith("UpdateItem")) {
          String compact = json.replaceAll("\\s+", "");
          String table = between(compact, "\"TableName\":\"", "\"");
          String key = between(compact, "\"y_id\":{\"S\":\"", "\"");
          if (key != null) {
            store.computeIfAbsent(table == null ? TABLE : table, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(key, k -> new HashMap<>(Map.of("y_id", k)));
          }
          response = "{}";
        } else {
          response = "{}";
        }
      } catch (Exception e) {
        code = 500;
        response = "{\"error\":\"" + e.getMessage().replace('"', '\'') + "\"}";
      }

      // Count any non-2xx coming out of the peer so we can distinguish "client never tried
      // the peer" (the bug) from "peer was tried and itself misbehaved" (a test bug).
      if (code >= 400 && PEER_HOST.equals(logicalHost)) {
        peerFailureCount.incrementAndGet();
      }

      byte[] out = response.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", contentType);
      exchange.sendResponseHeaders(code, out.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(out);
      }
    }
  }

  // ---------------- Tiny JSON helpers (kept local to avoid a dependency) ----------------

  private static byte[] readAll(InputStream is) throws IOException {
    return is.readAllBytes();
  }

  private static String between(String s, String start, String end) {
    int i = s.indexOf(start);
    if (i < 0) {
      return null;
    }
    i += start.length();
    int j = s.indexOf(end, i);
    if (j < 0) {
      return null;
    }
    return s.substring(i, j);
  }

  private static String toDynamoItemJson(Map<String, String> item) {
    StringBuilder sb = new StringBuilder("{\"Item\":{");
    boolean first = true;
    for (Map.Entry<String, String> e : item.entrySet()) {
      if (!first) {
        sb.append(',');
      }
      sb.append('\"').append(e.getKey()).append("\":{\"S\":\"")
          .append(escape(e.getValue())).append("\"}");
      first = false;
    }
    sb.append("}}");
    return sb.toString();
  }

  private static String escape(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
