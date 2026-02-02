/*
 * Copyright (c) 2015-2026 YCSB contributors. All Rights Reserved.
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

import org.apache.log4j.Logger;
import software.amazon.awssdk.annotations.NotNull;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

final class AlternatorLoadBalancer implements Closeable {

  private static final Logger LOGGER = Logger.getLogger(AlternatorLoadBalancer.class);

  private final Config config;
  private final List<URI> nodes;
  private final Map<URI, NodeFailure> failedNodes;
  private final AtomicInteger nodeIndex;
  private final Stats stats;
  private final HttpClient httpClient;
  private final ScheduledExecutorService scheduler;

  private volatile Instant lastSuccessfulRefresh;
  private final AtomicInteger consecutiveFailures;

  private AlternatorLoadBalancer(Config config) {
    this.config = config;
    this.nodes = new CopyOnWriteArrayList<>(List.of(config.seedUri()));
    this.failedNodes = new ConcurrentHashMap<>();
    this.nodeIndex = new AtomicInteger();
    this.stats = new Stats();
    this.lastSuccessfulRefresh = Instant.now();
    this.consecutiveFailures = new AtomicInteger();
    this.httpClient = createHttpClient(config.trustAllCertificates());
    this.scheduler = Executors.newSingleThreadScheduledExecutor(
        r -> Thread.ofVirtual().name("alternator-lb-refresh").unstarted(r));
  }

  static Builder builder() {
    return new Builder();
  }

  void start() {
    scheduler.scheduleWithFixedDelay(
        this::safeRefresh,
        0,
        config.refreshInterval().toMillis(),
        TimeUnit.MILLISECONDS
    );
    LOGGER.info("Load balancer started with " + nodes.size() + " initial node(s)");
  }

  @Override
  public void close() {
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(config.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
        scheduler.shutdownNow();
        LOGGER.warn("Forced shutdown of refresh scheduler");
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
    LOGGER.info("Load balancer stopped. " + stats);
  }

  URI nextNode() {
    stats.requests.incrementAndGet();
    cleanupExpiredFailures();

    var available = availableNodes();
    if (available.isEmpty()) {
      logDebug(() -> "No healthy nodes, falling back to seed");
      return config.seedUri();
    }

    var idx = Math.abs(nodeIndex.getAndIncrement() % available.size());
    return available.get(idx);
  }

  void markNodeFailed(URI node) {
    var failure = failedNodes.compute(node, (uri, existing) ->
        existing == null
            ? new NodeFailure(1, Instant.now())
            : new NodeFailure(existing.count() + 1, Instant.now()));

    if (failure.count() >= config.maxFailures()) {
      LOGGER.warn("Node " + node + " marked unhealthy after " + failure.count() + " failures");
    }
  }

  void markNodeSuccess(URI node) {
    failedNodes.remove(node);
  }

  int getNodeCount() {
    return nodes.size();
  }

  int getHealthyNodeCount() {
    return availableNodes().size();
  }

  Duration timeSinceLastRefresh() {
    return Duration.between(lastSuccessfulRefresh, Instant.now());
  }

  private List<URI> availableNodes() {
    var now = Instant.now();
    var expiry = config.failureExpiry();
    var maxFail = config.maxFailures();

    return nodes.stream()
        .filter(node -> {
          var failure = failedNodes.get(node);
          return failure == null
              || failure.count() < maxFail
              || Duration.between(failure.lastFailure(), now).compareTo(expiry) > 0;
        })
        .toList();
  }

  private void cleanupExpiredFailures() {
    var now = Instant.now();
    var expiry = config.failureExpiry();
    failedNodes.entrySet().removeIf(e ->
        Duration.between(e.getValue().lastFailure(), now).compareTo(expiry) > 0);
  }

  private void safeRefresh() {
    try {
      refresh();
    } catch (Exception e) {
      LOGGER.error("Unexpected refresh error", e);
      stats.failedRefreshes.incrementAndGet();
      consecutiveFailures.incrementAndGet();
    }
  }

  private void refresh() {
    var endpoint = buildLocalNodesUri(selectRefreshNode());

    try {
      var request = HttpRequest.newBuilder()
          .uri(endpoint)
          .timeout(config.httpTimeout())
          .GET()
          .build();

      var response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

      if (response.statusCode() != HttpURLConnection.HTTP_OK) {
        handleFailure("HTTP " + response.statusCode() + " from " + endpoint);
        return;
      }

      var newNodes = parseNodeList(response.body());
      if (newNodes.isEmpty()) {
        handleFailure("Empty node list from " + endpoint);
        return;
      }

      updateNodes(newNodes);
      stats.successfulRefreshes.incrementAndGet();
      lastSuccessfulRefresh = Instant.now();
      consecutiveFailures.set(0);

      logDebug(() -> "Refreshed: " + nodes.size() + " nodes, " + getHealthyNodeCount() + " healthy");

    } catch (IOException e) {
      handleFailure("IO error: " + e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private URI selectRefreshNode() {
    var available = availableNodes();
    if (available.isEmpty()) {
      return config.seedUri();
    }
    return available.get(Math.abs(nodeIndex.get() % available.size()));
  }

  private void handleFailure(String reason) {
    stats.failedRefreshes.incrementAndGet();
    var failures = consecutiveFailures.incrementAndGet();

    if (failures <= 3) {
      logDebug(() -> "Refresh failed: " + reason);
    } else if (failures == 4) {
      LOGGER.warn("Refresh failed " + failures + " times: " + reason);
    }
  }

  private void updateNodes(List<URI> newNodes) {
    var currentSet = new HashSet<>(nodes);
    var newSet = new HashSet<>(newNodes);

    if (!currentSet.equals(newSet)) {
      nodes.clear();
      nodes.addAll(newNodes);
      failedNodes.keySet().retainAll(newSet);
      LOGGER.info("Node list updated: " + newNodes.size() + " nodes");
    }
  }

  private URI buildLocalNodesUri(URI base) {
    var query = Stream.of(
            config.datacenter().isEmpty() ? null : "dc=" + config.datacenter(),
            config.rack().isEmpty() ? null : "rack=" + config.rack()
        )
        .filter(Objects::nonNull)
        .reduce((a, b) -> a + "&" + b)
        .orElse(null);

    try {
      return new URI(base.getScheme(), null, base.getHost(), base.getPort(), "/localnodes", query, null);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Invalid URI construction", e);
    }
  }

  private List<URI> parseNodeList(InputStream body) throws IOException {
    String content;
    try (var scanner = new Scanner(body).useDelimiter("\\A")) {
      content = scanner.hasNext() ? scanner.next().trim() : "";
    }

    if (!content.startsWith("[") || !content.endsWith("]")) {
      return List.of();
    }

    var inner = content.substring(1, content.length() - 1);
    var result = new ArrayList<URI>();

    for (var part : inner.split(",")) {
      var host = part.trim();
      if (host.isEmpty()) {
        continue;
      }
      if (host.startsWith("\"") && host.endsWith("\"")) {
        host = host.substring(1, host.length() - 1);
      }
      try {
        result.add(new URI(config.scheme(), null, host, config.port(), null, null, null));
      } catch (URISyntaxException e) {
        LOGGER.warn("Invalid host: " + host, e);
      }
    }
    return result;
  }

  private void logDebug(Supplier<String> message) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(message.get());
    }
  }

  private static HttpClient createHttpClient(boolean trustAll) {
    var builder = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .executor(Executors.newVirtualThreadPerTaskExecutor());

    if (trustAll) {
      try {
        var ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[]{InsecureTrustManager.INSTANCE}, new SecureRandom());
        builder.sslContext(ctx);
      } catch (NoSuchAlgorithmException | KeyManagementException e) {
        throw new IllegalStateException("Failed to create insecure SSL context", e);
      }
    }
    return builder.build();
  }

  private record NodeFailure(int count, Instant lastFailure) {
  }

  private record Stats(AtomicLong requests, AtomicLong successfulRefreshes, AtomicLong failedRefreshes) {
    Stats() {
      this(new AtomicLong(), new AtomicLong(), new AtomicLong());
    }

    @Override
    @NotNull
    public String toString() {
      var total = successfulRefreshes.get() + failedRefreshes.get();
      return "requests=" + requests.get() + ", refreshes=" + successfulRefreshes.get() + "/" + total;
    }
  }

  private record Config(
      URI seedUri,
      String scheme,
      int port,
      String datacenter,
      String rack,
      boolean trustAllCertificates,
      Duration refreshInterval,
      Duration httpTimeout,
      Duration failureExpiry,
      Duration shutdownTimeout,
      int maxFailures
  ) {
    static final Duration DEFAULT_REFRESH = Duration.ofSeconds(5);
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);
    static final Duration DEFAULT_EXPIRY = Duration.ofMinutes(1);
    static final Duration DEFAULT_SHUTDOWN = Duration.ofSeconds(5);
    static final int DEFAULT_MAX_FAILURES = 3;
  }

  private enum InsecureTrustManager implements X509TrustManager {
    INSTANCE;

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  static final class Builder {
    private URI seedUri;
    private String datacenter = "";
    private String rack = "";
    private boolean trustAllCertificates;
    private Duration refreshInterval = Config.DEFAULT_REFRESH;
    private Duration httpTimeout = Config.DEFAULT_TIMEOUT;

    private Builder() {
    }

    Builder seedUri(URI uri) {
      this.seedUri = Objects.requireNonNull(uri, "seedUri");
      if (uri.getScheme() == null || uri.getHost() == null || uri.getPort() < 0) {
        throw new IllegalArgumentException("Invalid seed URI: " + uri);
      }
      return this;
    }

    Builder datacenter(String dc) {
      this.datacenter = Optional.ofNullable(dc).orElse("");
      return this;
    }

    Builder rack(String r) {
      this.rack = Optional.ofNullable(r).orElse("");
      return this;
    }

    Builder trustAllCertificates(boolean trust) {
      this.trustAllCertificates = trust;
      return this;
    }

    Builder refreshInterval(Duration interval) {
      if (interval != null && !interval.isNegative() && !interval.isZero()) {
        this.refreshInterval = interval;
      }
      return this;
    }

    Builder httpTimeout(Duration timeout) {
      if (timeout != null && !timeout.isNegative() && !timeout.isZero()) {
        this.httpTimeout = timeout;
      }
      return this;
    }

    AlternatorLoadBalancer build() {
      Objects.requireNonNull(seedUri, "seedUri is required");
      var config = new Config(
          seedUri,
          seedUri.getScheme(),
          seedUri.getPort(),
          datacenter,
          rack,
          trustAllCertificates,
          refreshInterval,
          httpTimeout,
          Config.DEFAULT_EXPIRY,
          Config.DEFAULT_SHUTDOWN,
          Config.DEFAULT_MAX_FAILURES
      );
      return new AlternatorLoadBalancer(config);
    }
  }
}
