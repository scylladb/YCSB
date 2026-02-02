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
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointParams;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class AlternatorEndpointProviderTest {

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
  public void resolveEndpointReturnsCompletedFuture() {
    mockServer.createContext("/localnodes", exchange -> {
      String response = "[\"10.0.0.1\"]";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (AlternatorEndpointProvider provider = AlternatorEndpointProvider.builder()
        .seedUri(seed)
        .build()) {

      CompletableFuture<Endpoint> future = provider.resolveEndpoint(
          DynamoDbEndpointParams.builder().build());

      assertThat(future, is(notNullValue()));
      assertTrue(future.isDone());

      Endpoint endpoint = future.join();
      assertThat(endpoint, is(notNullValue()));
      assertThat(endpoint.url(), is(notNullValue()));
    }
  }

  @Test
  public void cachesEndpointFutures() throws Exception {
    mockServer.createContext("/localnodes", exchange -> {
      String response = "[\"10.0.0.1\"]";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (AlternatorEndpointProvider provider = AlternatorEndpointProvider.builder()
        .seedUri(seed)
        .build()) {

      Thread.sleep(200);

      CompletableFuture<Endpoint> first = provider.resolveEndpoint(
          DynamoDbEndpointParams.builder().build());
      CompletableFuture<Endpoint> second = provider.resolveEndpoint(
          DynamoDbEndpointParams.builder().build());

      assertThat(first.join().url(), is(second.join().url()));
    }
  }

  @Test
  public void distributesAcrossMultipleNodes() throws Exception {
    mockServer.createContext("/localnodes", exchange -> {
      String response = "[\"10.0.0.1\", \"10.0.0.2\", \"10.0.0.3\"]";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (AlternatorEndpointProvider provider = AlternatorEndpointProvider.builder()
        .seedUri(seed)
        .build()) {

      Thread.sleep(200);

      Set<String> hosts = new HashSet<>();
      for (int i = 0; i < 9; i++) {
        Endpoint endpoint = provider.resolveEndpoint(
            DynamoDbEndpointParams.builder().build()).join();
        hosts.add(endpoint.url().getHost());
      }

      assertThat(hosts.size(), is(3));
    }
  }

  @Test
  public void builderConfiguresDatacenterAndRack() throws Exception {
    StringBuilder receivedQuery = new StringBuilder();

    mockServer.createContext("/localnodes", exchange -> {
      if (receivedQuery.isEmpty()) {
        receivedQuery.append(exchange.getRequestURI().getQuery());
      }
      String response = "[\"10.0.0.1\"]";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    try (AlternatorEndpointProvider provider = AlternatorEndpointProvider.builder()
        .seedUri(seed)
        .datacenter("dc1")
        .rack("rack1")
        .build()) {

      Thread.sleep(200);

      String query = receivedQuery.toString();
      assertTrue(query.contains("dc=dc1"));
      assertTrue(query.contains("rack=rack1"));
    }
  }

  @Test
  public void closeReleasesResources() throws Exception {
    mockServer.createContext("/localnodes", exchange -> {
      String response = "[\"10.0.0.1\"]";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    mockServer.start();

    URI seed = URI.create("http://localhost:" + serverPort);
    AlternatorEndpointProvider provider = AlternatorEndpointProvider.builder()
        .seedUri(seed)
        .build();

    provider.resolveEndpoint(DynamoDbEndpointParams.builder().build());
    provider.close();

    Thread.sleep(100);
    int count = countActiveRefreshThreads();
    Thread.sleep(6000);
    int countAfter = countActiveRefreshThreads();

    assertThat(countAfter, is(count));
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
