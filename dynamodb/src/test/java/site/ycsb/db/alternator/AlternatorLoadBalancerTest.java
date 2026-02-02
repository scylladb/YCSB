/*
 * Copyright (c) 2015-2016 YCSB contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package site.ycsb.db.alternator;

import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class AlternatorLoadBalancerTest {

  private HttpServer mockServer;
  private int serverPort;

  @Before
  public void setUp() throws IOException {
    mockServer = HttpServer.create(new InetSocketAddress(0), 0);
    serverPort = mockServer.getAddress().getPort();
  }

  @After
  public void tearDown() {
    if (mockServer != null) {
      mockServer.stop(0);
    }
  }

  @Test
  public void builderRequiresSeedUri() {
    try {
      AlternatorLoadBalancer.builder().build();
      fail("Expected NullPointerException");
    } catch (NullPointerException e) {
      assertThat(e.getMessage(), containsString("seedUri"));
    }
  }

  @Test
  public void builderRejectsInvalidUri() {
    try {
      AlternatorLoadBalancer.builder()
          .seedUri(URI.create("invalid"))
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("Invalid seed URI"));
    }
  }

  @Test
  public void nextNodeReturnsInitialSeedWhenNoRefreshHappened() {
    URI seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createLoadBalancer(seed)) {
      URI node = lb.nextNode();
      assertThat(node.getHost(), is("localhost"));
      assertThat(node.getPort(), is(serverPort));
    }
  }

  @Test
  public void roundRobinDistributesAcrossNodes() throws Exception {
    setupMockLocalNodes("[\"10.0.0.1\", \"10.0.0.2\", \"10.0.0.3\"]");
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);

      Set<String> hosts = new HashSet<>();
      for (int i = 0; i < 9; i++) {
        hosts.add(lb.nextNode().getHost());
      }

      assertThat(hosts.size(), is(3));
      assertTrue(hosts.contains("10.0.0.1"));
      assertTrue(hosts.contains("10.0.0.2"));
      assertTrue(hosts.contains("10.0.0.3"));
    }
  }

  @Test
  public void handlesEmptyNodeListGracefully() throws Exception {
    setupMockLocalNodes("[]");
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);
      URI node = lb.nextNode();
      assertThat(node.getHost(), is("localhost"));
    }
  }

  @Test
  public void handlesMalformedResponseGracefully() throws Exception {
    mockServer.createContext("/localnodes", exchange -> {
      String response = "not valid json";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);
      URI node = lb.nextNode();
      assertThat(node.getHost(), is("localhost"));
    }
  }

  @Test
  public void handlesServerErrorGracefully() throws Exception {
    mockServer.createContext("/localnodes", exchange -> {
      exchange.sendResponseHeaders(500, 0);
      exchange.getResponseBody().close();
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);
      URI node = lb.nextNode();
      assertThat(node.getHost(), is("localhost"));
    }
  }

  @Test
  public void passesDatacenterAndRackInQuery() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    StringBuilder receivedQuery = new StringBuilder();

    mockServer.createContext("/localnodes", exchange -> {
      receivedQuery.append(exchange.getRequestURI().getQuery());
      latch.countDown();
      String response = "[\"10.0.0.1\"]";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (var lb = AlternatorLoadBalancer.builder()
        .seedUri(seed)
        .datacenter("dc1")
        .rack("rack1")
        .build()) {
      lb.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS));

      String query = receivedQuery.toString();
      assertTrue(query.contains("dc=dc1"));
      assertTrue(query.contains("rack=rack1"));
    }
  }

  @Test
  public void updatesNodeListOnRefresh() throws Exception {
    int[] callCount = {0};

    mockServer.createContext("/localnodes", exchange -> {
      callCount[0]++;
      String response = callCount[0] == 1
          ? "[\"localhost\"]"
          : "[\"localhost\", \"10.0.0.2\"]";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(3000);
      assertTrue("Expected at least 2 refresh calls, got " + callCount[0], callCount[0] >= 2);
      assertThat(lb.getNodeCount(), is(2));

      Set<String> hosts = new HashSet<>();
      for (int i = 0; i < 6; i++) {
        hosts.add(lb.nextNode().getHost());
      }
      assertThat(hosts.size(), is(2));
    }
  }

  @Test
  public void closeStopsRefreshThread() throws Exception {
    setupMockLocalNodes("[\"10.0.0.1\"]");
    mockServer.start();

    var seed = URI.create("http://localhost:" + serverPort);
    var lb = createAndStartLoadBalancer(seed);

    Thread.sleep(100);
    lb.close();
    Thread.sleep(100);

    int beforeCount = countActiveRefreshThreads();
    Thread.sleep(100);
    int afterCount = countActiveRefreshThreads();

    assertThat(afterCount, is(beforeCount));
  }

  @Test
  public void markNodeFailedExcludesNodeAfterThreshold() throws Exception {
    setupMockLocalNodes("[\"10.0.0.1\", \"10.0.0.2\"]");
    mockServer.start();

    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);

      var failedNode = URI.create("http://10.0.0.1:" + serverPort);
      lb.markNodeFailed(failedNode);
      lb.markNodeFailed(failedNode);
      lb.markNodeFailed(failedNode);
      assertThat(lb.getHealthyNodeCount(), is(1));
    }
  }

  @Test
  public void markNodeSuccessResetsFailureCount() throws Exception {
    setupMockLocalNodes("[\"10.0.0.1\", \"10.0.0.2\"]");
    mockServer.start();

    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);

      var node = URI.create("http://10.0.0.1:" + serverPort);
      lb.markNodeFailed(node);
      lb.markNodeFailed(node);
      lb.markNodeSuccess(node);

      assertThat(lb.getHealthyNodeCount(), is(2));
    }
  }

  @Test
  public void fallsBackToSeedWhenAllNodesFail() throws Exception {
    setupMockLocalNodes("[\"10.0.0.1\"]");
    mockServer.start();

    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);

      var node = URI.create("http://10.0.0.1:" + serverPort);
      for (int i = 0; i < 5; i++) {
        lb.markNodeFailed(node);
      }

      var selected = lb.nextNode();
      assertThat(selected.getHost(), is("localhost"));
    }
  }

  @Test
  public void getNodeCountReturnsCorrectCount() throws Exception {
    setupMockLocalNodes("[\"10.0.0.1\", \"10.0.0.2\", \"10.0.0.3\"]");
    mockServer.start();

    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);

      assertThat(lb.getNodeCount(), is(3));
    }
  }

  @Test
  public void timeSinceLastRefreshReturnsValidDuration() throws Exception {
    setupMockLocalNodes("[\"10.0.0.1\"]");
    mockServer.start();

    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = createAndStartLoadBalancer(seed)) {
      Thread.sleep(200);

      var duration = lb.timeSinceLastRefresh();
      assertTrue(duration.toMillis() >= 0);
      assertTrue(duration.toMillis() < 5000);
    }
  }

  @Test
  public void builderAcceptsCustomRefreshInterval() {
    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = AlternatorLoadBalancer.builder()
        .seedUri(seed)
        .refreshInterval(java.time.Duration.ofSeconds(10))
        .build()) {
      assertNotNull(lb);
    }
  }

  @Test
  public void builderAcceptsCustomHttpTimeout() {
    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = AlternatorLoadBalancer.builder()
        .seedUri(seed)
        .httpTimeout(java.time.Duration.ofSeconds(30))
        .build()) {
      assertNotNull(lb);
    }
  }

  @Test
  public void builderIgnoresNullOrInvalidDurations() {
    var seed = URI.create("http://localhost:" + serverPort);
    try (var lb = AlternatorLoadBalancer.builder()
        .seedUri(seed)
        .refreshInterval(null)
        .refreshInterval(java.time.Duration.ZERO)
        .refreshInterval(java.time.Duration.ofSeconds(-1))
        .httpTimeout(null)
        .build()) {
      assertNotNull(lb);
    }
  }

  private void setupMockLocalNodes(String jsonArray) {
    mockServer.createContext("/localnodes", exchange -> {
      exchange.sendResponseHeaders(200, jsonArray.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(jsonArray.getBytes());
      }
    });
  }

  private AlternatorLoadBalancer createLoadBalancer(URI seed) {
    return AlternatorLoadBalancer.builder()
        .seedUri(seed)
        .refreshInterval(java.time.Duration.ofSeconds(1))
        .build();
  }

  private AlternatorLoadBalancer createAndStartLoadBalancer(URI seed) {
    var lb = createLoadBalancer(seed);
    lb.start();
    return lb;
  }

  private int countActiveRefreshThreads() {
    int count = 0;
    for (Thread t : Thread.getAllStackTraces().keySet()) {
      if (t.getName().contains("alternator-node-refresh") && t.isAlive()) {
        count++;
      }
    }
    return count;
  }
}
