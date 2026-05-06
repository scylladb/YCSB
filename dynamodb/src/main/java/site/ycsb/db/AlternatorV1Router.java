/*
 * Copyright (c) 2026 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */

package site.ycsb.db;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Client-side router for ScyllaDB Alternator over the AWS SDK v1.
 *
 * <p>Mirrors the behaviour of the v2 {@code AlternatorDynamoDbAsyncClient}: periodically polls
 * {@code /localnodes} on the seed endpoint to discover live nodes (optionally scoped to a
 * datacenter and/or rack), maintains one {@link AmazonDynamoDB} per IP, and hands out a
 * different client per request via round-robin. The list of clients is swapped atomically on
 * each refresh so callers always see a consistent snapshot.
 */
final class AlternatorV1Router {

  private static final Logger LOGGER = Logger.getLogger(AlternatorV1Router.class);
  private static final Pattern QUOTED_IP = Pattern.compile("\"([^\"]+)\"");
  private static final long REFRESH_PERIOD_SECONDS = 1L;

  private final URI seed;
  private final int targetPort;
  private final String scheme;
  private final String region;
  private final String datacenter;
  private final String rack;
  private final ClientConfiguration clientConfiguration;
  private final AWSCredentialsProvider credentials;

  private final ConcurrentHashMap<String, AmazonDynamoDB> clientsByIp = new ConcurrentHashMap<>();
  private volatile List<AmazonDynamoDB> snapshot;
  private final AtomicInteger cursor = new AtomicInteger();
  private final ScheduledExecutorService refresher;
  private volatile boolean closed = false;

  AlternatorV1Router(URI seed, int targetPort, String region, String datacenter, String rack,
                     ClientConfiguration clientConfiguration, AWSCredentialsProvider credentials) {
    this.seed = seed;
    this.targetPort = targetPort > 0 ? targetPort : seed.getPort();
    this.scheme = seed.getScheme() == null ? "http" : seed.getScheme();
    this.region = region;
    this.datacenter = datacenter;
    this.rack = rack;
    this.clientConfiguration = clientConfiguration;
    this.credentials = credentials;

    // Seed with the configured node so the first requests have somewhere to go even before the
    // first /localnodes response arrives.
    var seedHost = seed.getHost();
    if (seedHost == null || seedHost.isEmpty()) {
      throw new IllegalArgumentException("dynamodb.endpoint must include a host for Alternator load balancing");
    }
    addClient(seedHost);
    rebuildSnapshot();

    this.refresher = Executors.newSingleThreadScheduledExecutor(r -> {
      var t = new Thread(r, "alternator-v1-refresher");
      t.setDaemon(true);
      return t;
    });
    refresher.scheduleWithFixedDelay(this::refresh,
        REFRESH_PERIOD_SECONDS, REFRESH_PERIOD_SECONDS, TimeUnit.SECONDS);
  }

  AmazonDynamoDB nextClient() {
    var current = snapshot;
    if (current == null || current.isEmpty()) {
      throw new IllegalStateException("Alternator router has no live nodes");
    }
    var idx = Math.floorMod(cursor.getAndIncrement(), current.size());
    return current.get(idx);
  }

  void close() {
    closed = true;
    refresher.shutdownNow();
    for (var c : clientsByIp.values()) {
      try {
        c.shutdown();
      } catch (Exception ignored) {
        // best-effort
      }
    }
    clientsByIp.clear();
    snapshot = Collections.emptyList();
  }

  private void refresh() {
    if (closed) {
      return;
    }
    // Always query the seed for /localnodes. The seed is treated as a stable bootstrap endpoint
    // (typically a DNS hostname or load balancer that fronts the cluster), so it remains
    // reachable even after the discovered set churns. If the seed becomes unreachable, fall back
    // to one of the currently-live IPs so refreshes don't permanently stall.
    try {
      var ips = fetchLocalNodes(seed.getHost());
      if (!ips.isEmpty()) {
        reconcile(ips);
        return;
      }
    } catch (Throwable t) {
      LOGGER.warn("Alternator /localnodes refresh via seed failed: " + t.getMessage());
    }

    var fallbacks = new ArrayList<>(clientsByIp.keySet());
    fallbacks.remove(seed.getHost());
    for (var ip : fallbacks) {
      try {
        var ips = fetchLocalNodes(ip);
        if (!ips.isEmpty()) {
          reconcile(ips);
          return;
        }
      } catch (Throwable ignored) {
        // try next
      }
    }
  }

  private List<String> fetchLocalNodes(String host) throws Exception {
    var path = "/localnodes";
    var query = new StringBuilder();
    if (datacenter != null && !datacenter.isEmpty()) {
      query.append("dc=").append(URLEncoder.encode(datacenter, StandardCharsets.UTF_8));
    }
    if (rack != null && !rack.isEmpty()) {
      if (query.length() > 0) {
        query.append("&");
      }
      query.append("rack=").append(URLEncoder.encode(rack, StandardCharsets.UTF_8));
    }
    var url = URI.create(scheme + "://" + host + ":" + targetPort + path
        + (query.length() == 0 ? "" : "?" + query)).toURL();
    var conn = (HttpURLConnection) url.openConnection();
    conn.setConnectTimeout(2000);
    conn.setReadTimeout(2000);
    conn.setRequestMethod("GET");
    try (var reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
      var sb = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
      return parseIps(sb.toString());
    } finally {
      conn.disconnect();
    }
  }

  private static List<String> parseIps(String json) {
    // Response is a JSON array of strings, e.g. ["10.0.0.1","10.0.0.2"]. A simple regex for
    // quoted tokens is sufficient and avoids pulling in a JSON parser for the v1 binding.
    var out = new ArrayList<String>();
    Matcher m = QUOTED_IP.matcher(json);
    while (m.find()) {
      out.add(m.group(1));
    }
    return out;
  }

  private synchronized void reconcile(List<String> liveIps) {
    var live = new HashMap<String, Boolean>();
    for (var ip : liveIps) {
      live.put(ip, Boolean.TRUE);
      if (!clientsByIp.containsKey(ip)) {
        addClient(ip);
      }
    }
    // Drop clients that are no longer live.
    var iterator = clientsByIp.entrySet().iterator();
    while (iterator.hasNext()) {
      var entry = iterator.next();
      if (!live.containsKey(entry.getKey())) {
        try {
          entry.getValue().shutdown();
        } catch (Exception ignored) {
          // best-effort
        }
        iterator.remove();
      }
    }
    rebuildSnapshot();
  }

  private void addClient(String ip) {
    var endpoint = scheme + "://" + ip + ":" + targetPort;
    var builder = AmazonDynamoDBClientBuilder.standard()
        .withClientConfiguration(clientConfiguration)
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
    if (credentials != null) {
      builder = builder.withCredentials(credentials);
    }
    clientsByIp.put(ip, builder.build());
    LOGGER.info("Alternator v1 router: added node " + endpoint);
  }

  private void rebuildSnapshot() {
    snapshot = List.copyOf(clientsByIp.values());
  }
}
