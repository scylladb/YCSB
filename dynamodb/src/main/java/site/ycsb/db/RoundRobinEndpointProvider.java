/*
 * Copyright 2015-2026 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 */

package site.ycsb.db;

import org.apache.log4j.Logger;
import software.amazon.awssdk.endpoints.Endpoint;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointParams;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Endpoint provider that discovers all IPs behind a hostname at startup and
 * distributes requests across them in round-robin order.
 *
 * <p><b>Init:</b> repeatedly queries DNS until the resolved set has been stable
 * (no new IPs) for {@code stableRoundsNeeded} consecutive rounds. Each round
 * waits {@code discoveryDelayMs} before the next query, giving DNS time to
 * return a different entry if it does per-query rotation.
 *
 * <p><b>Periodic refresh:</b> every {@code refreshIntervalSeconds}, one DNS
 * query is issued. If it returns an IP not yet in the pool, that IP is added
 * and will start receiving traffic on the next request cycle. If all IPs are
 * already known, nothing happens — no lock, no list swap, no log noise.
 */
final class RoundRobinEndpointProvider implements DynamoDbEndpointProvider, AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(RoundRobinEndpointProvider.class);

  private final String scheme;
  private final String hostname;
  private final int port;
  private final AtomicInteger counter = new AtomicInteger(0);
  private final AtomicReference<List<URI>> endpoints = new AtomicReference<>(List.of());
  private final ScheduledExecutorService scheduler;

  RoundRobinEndpointProvider(URI seedEndpoint, long refreshIntervalSeconds,
                              int stableRoundsNeeded, long discoveryDelayMs) {
    this.scheme = seedEndpoint.getScheme();
    this.hostname = seedEndpoint.getHost();
    this.port = seedEndpoint.getPort();

    discover(stableRoundsNeeded, discoveryDelayMs);

    if (endpoints.get().isEmpty()) {
      throw new IllegalStateException("DNS discovery returned no addresses for: " + hostname);
    }

    scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      var t = new Thread(r, "ycsb-dns-refresh");
      t.setDaemon(true);
      return t;
    });
    scheduler.scheduleAtFixedRate(this::refresh, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);
  }

  /**
   * Queries DNS repeatedly until no new IPs have appeared for {@code stableRoundsNeeded}
   * consecutive rounds. Waits {@code delayMs} between rounds so that DNS servers
   * doing per-query rotation have time to return a different entry.
   */
  private void discover(int stableRoundsNeeded, long delayMs) {
    var known = new LinkedHashSet<URI>();
    var stableRounds = 0;
    var totalQueries = 0;

    LOGGER.info("DNS discovery starting for " + hostname
        + " (stable after " + stableRoundsNeeded + " consecutive empty rounds, delay=" + delayMs + "ms)");

    while (stableRounds < stableRoundsNeeded) {
      var resolved = dnsQuery();
      totalQueries++;

      var sizeBefore = known.size();
      known.addAll(resolved);

      if (known.size() > sizeBefore) {
        LOGGER.info("Round " + totalQueries + ": +" + (known.size() - sizeBefore)
            + " new IP(s) -> " + known);
        stableRounds = 0;
      } else {
        stableRounds++;
      }

      if (stableRounds < stableRoundsNeeded) {
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    endpoints.set(List.copyOf(known));
    LOGGER.info("DNS discovery done after " + totalQueries + " queries, endpoints: " + endpoints.get());
  }

  /**
   * Checks DNS for new IPs. If any are found they are appended to the live
   * endpoint list atomically. If nothing changed, returns immediately.
   */
  private void refresh() {
    var resolved = dnsQuery();
    if (resolved.isEmpty()) {
      return;
    }

    var current = endpoints.get();
    var newOnes = resolved.stream()
        .filter(u -> !current.contains(u))
        .toList();

    if (newOnes.isEmpty()) {
      return;
    }

    var updated = new ArrayList<>(current);
    updated.addAll(newOnes);
    endpoints.set(List.copyOf(updated));
    LOGGER.info("New endpoint(s) added for " + hostname + ": " + newOnes
        + " -> pool size now " + updated.size());
  }

  private List<URI> dnsQuery() {
    try {
      return Arrays.stream(InetAddress.getAllByName(hostname))
          .map(addr -> {
            var host = addr instanceof Inet6Address
                ? "[" + addr.getHostAddress() + "]"
                : addr.getHostAddress();
            return URI.create(scheme + "://" + host + ":" + port);
          })
          .distinct()
          .toList();
    } catch (UnknownHostException e) {
      LOGGER.warn("DNS query failed for '" + hostname + "': " + e.getMessage());
      return List.of();
    }
  }

  @Override
  public CompletableFuture<Endpoint> resolveEndpoint(DynamoDbEndpointParams params) {
    var current = endpoints.get();
    var uri = current.get(Math.floorMod(counter.getAndIncrement(), current.size()));
    return CompletableFuture.completedFuture(Endpoint.builder().url(uri).build());
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
  }
}
