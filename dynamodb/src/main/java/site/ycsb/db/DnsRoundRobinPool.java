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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Generic DNS-backed round-robin pool.
 *
 * <p>Holds one item (e.g. {@code DynamoDbAsyncClient}) per discovered IP address and
 * distributes calls to {@link #next()} across them in strict round-robin order.
 *
 * <h3>Why one item per IP instead of an endpoint provider</h3>
 * Using the AWS SDK's {@code endpointProvider} hook to rotate IPs is unreliable:
 * the SDK may cache the resolved endpoint between requests, and even when it doesn't,
 * Netty's channel pool can coalesce connections in ways that defeat IP rotation.
 * Creating one {@code DynamoDbAsyncClient} per IP with {@code endpointOverride} pointing
 * directly at that IP bypasses all of that: Netty maintains a separate, healthy connection
 * pool per client, and each call to {@link #next()} explicitly selects the next client.
 *
 * <h3>Discovery</h3>
 * On construction the pool queries DNS repeatedly (via the injected {@link DnsResolver})
 * until the resolved IP set has been stable — no new addresses seen — for
 * {@code stableRoundsNeeded} consecutive rounds. A configurable {@code discoveryDelayMs}
 * between rounds lets DNS servers that rotate one IP per query return different entries.
 * If DNS returns all A records in one call (the common case) the loop converges in
 * {@code stableRoundsNeeded + 1} total queries.
 *
 * <h3>Periodic refresh</h3>
 * A single daemon thread re-queries DNS every {@code refreshIntervalSeconds}. If a new IP
 * appears, a new item is created via the injected {@link ItemFactory} and appended to the
 * live list atomically. If nothing changed, the method returns immediately.
 * Items are never removed: connection pools stay warm and no in-flight requests are
 * interrupted if a node temporarily disappears from DNS.
 *
 * <h3>Thread safety</h3>
 * {@link #next()} is lock-free: an {@link AtomicInteger} counter selects the index, and
 * the item list is read from an {@link AtomicReference} snapshot that is only ever
 * replaced (never mutated). Safe for concurrent use by any number of YCSB threads.
 *
 * @param <T> type of item held in the pool (e.g. {@code DynamoDbAsyncClient})
 */
final class DnsRoundRobinPool<T> implements AutoCloseable {

  private static final Logger LOGGER = Logger.getLogger(DnsRoundRobinPool.class);

  // ---- Injectable interfaces -----------------------------------------------

  /**
   * Resolves a hostname to zero or more IP address strings.
   * The production implementation delegates to {@link InetAddress#getAllByName}.
   * Tests inject a lambda that returns controlled sequences of IPs.
   */
  @FunctionalInterface
  interface DnsResolver {
    List<String> resolve(String hostname) throws UnknownHostException;
  }

  /**
   * Creates one pool item for the given endpoint URI.
   * Called once per discovered IP during discovery and refresh.
   * Tests inject a lambda that returns a simple marker value (e.g. the IP string).
   */
  @FunctionalInterface
  interface ItemFactory<T> {
    T create(URI endpoint);
  }

  /** Production DNS resolver: returns every A/AAAA record for the hostname. */
  static DnsResolver systemResolver() {
    return hostname -> Arrays.stream(InetAddress.getAllByName(hostname))
        .map(addr -> addr instanceof Inet6Address
            ? "[" + addr.getHostAddress() + "]"
            : addr.getHostAddress())
        .distinct()
        .toList();
  }

  // ---- State ----------------------------------------------------------------

  private final String scheme;
  private final String hostname;
  private final int port;
  private final ItemFactory<T> factory;
  private final DnsResolver resolver;

  /**
   * Canonical store: IP address string → pool item, in insertion (discovery) order.
   * Only appended to; entries are never removed.
   * Modified exclusively from the constructor thread and the single scheduler thread.
   */
  private final LinkedHashMap<String, T> ipToItem = new LinkedHashMap<>();

  /**
   * Atomically published snapshot of {@code ipToItem.values()}.
   * {@link #next()} reads this without locking; the scheduler swaps it via
   * {@link AtomicReference#set} after updating {@code ipToItem}.
   */
  private final AtomicReference<List<T>> items = new AtomicReference<>(List.of());

  /** Monotonically increasing request counter. Shared across all calling threads. */
  private final AtomicInteger counter = new AtomicInteger(0);

  private final ScheduledExecutorService scheduler;

  // ---- Construction ---------------------------------------------------------

  DnsRoundRobinPool(URI seed,
                    ItemFactory<T> factory,
                    DnsResolver resolver,
                    long refreshIntervalSeconds,
                    int stableRoundsNeeded,
                    long discoveryDelayMs) {
    this.scheme = seed.getScheme();
    this.hostname = seed.getHost();
    this.port = seed.getPort();
    this.factory = factory;
    this.resolver = resolver;

    discover(stableRoundsNeeded, discoveryDelayMs);

    if (items.get().isEmpty()) {
      throw new IllegalStateException(
          "DNS discovery for '" + hostname + "' found no reachable addresses");
    }

    scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      var t = new Thread(r, "ycsb-dns-refresh");
      t.setDaemon(true);
      return t;
    });
    scheduler.scheduleAtFixedRate(
        this::refresh, refreshIntervalSeconds, refreshIntervalSeconds, TimeUnit.SECONDS);

    LOGGER.info("DnsRoundRobinPool ready: " + ipToItem.keySet()
        + ", refresh every " + refreshIntervalSeconds + "s");
  }

  // ---- Discovery ------------------------------------------------------------

  private void discover(int stableRoundsNeeded, long delayMs) {
    int stableRounds = 0;
    int totalQueries = 0;

    LOGGER.info("DNS discovery: hostname=" + hostname
        + " stableRoundsNeeded=" + stableRoundsNeeded
        + " delayMs=" + delayMs);

    while (stableRounds < stableRoundsNeeded) {
      var newIps = queryForNewIps();
      totalQueries++;

      if (newIps.isEmpty()) {
        stableRounds++;
      } else {
        for (var ip : newIps) {
          try {
            ipToItem.put(ip, factory.create(toUri(ip)));
          } catch (Exception e) {
            LOGGER.error("Factory failed for IP " + ip + " — skipping: " + e.getMessage(), e);
          }
        }
        publishSnapshot();
        LOGGER.info("Query #" + totalQueries + ": +" + newIps + " known=" + ipToItem.keySet());
        stableRounds = 0;
      }

      if (stableRounds < stableRoundsNeeded && delayMs > 0) {
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    LOGGER.info("Discovery done: " + totalQueries + " queries -> " + ipToItem.keySet());
  }

  // ---- Refresh (package-private so tests can trigger it synchronously) ------

  /**
   * Queries DNS once and adds items for any IPs not yet in the pool.
   * No-op if nothing changed — no allocation, no logging.
   * Package-private so unit tests can invoke it directly without waiting
   * for the scheduler.
   */
  void refresh() {
    var newIps = queryForNewIps();
    if (newIps.isEmpty()) {
      return;
    }

    for (var ip : newIps) {
      try {
        ipToItem.put(ip, factory.create(toUri(ip)));
      } catch (Exception e) {
        LOGGER.error("Factory failed for IP " + ip + " during refresh — skipping: " + e.getMessage(), e);
      }
    }
    publishSnapshot();
    LOGGER.info("Refresh: added " + newIps + " -> pool size=" + ipToItem.size());
  }

  // ---- Core API -------------------------------------------------------------

  /**
   * Returns the next item in round-robin order.
   * Lock-free and safe for concurrent use by any number of threads.
   *
   * <p>For operations that span multiple SDK calls (e.g. paginated scan), callers
   * should capture the result of a single {@code next()} call and reuse it across
   * all pages to avoid splitting a logical operation across different nodes.
   */
  T next() {
    var current = items.get();
    return current.get(Math.floorMod(counter.getAndIncrement(), current.size()));
  }

  /** Returns the number of items currently in the pool. */
  int size() {
    return items.get().size();
  }

  /**
   * Returns current known IPs in discovery order.
   * Intended for logging and testing; not for hot paths.
   */
  List<String> currentIps() {
    return new ArrayList<>(ipToItem.keySet());
  }

  // ---- Lifecycle ------------------------------------------------------------

  @Override
  public void close() {
    scheduler.shutdownNow();
    for (var item : ipToItem.values()) {
      if (item instanceof AutoCloseable ac) {
        try {
          ac.close();
        } catch (Exception e) {
          LOGGER.warn("Error closing pool item for host " + hostname, e);
        }
      }
    }
    LOGGER.info("DnsRoundRobinPool closed: " + hostname);
  }

  // ---- Helpers --------------------------------------------------------------

  private List<String> queryForNewIps() {
    List<String> resolved;
    try {
      resolved = resolver.resolve(hostname);
    } catch (UnknownHostException e) {
      LOGGER.warn("DNS query failed for '" + hostname + "': " + e.getMessage());
      return List.of();
    }
    return resolved.stream()
        .filter(ip -> !ipToItem.containsKey(ip))
        .toList();
  }

  private URI toUri(String ip) {
    return URI.create(scheme + "://" + ip + ":" + port);
  }

  private void publishSnapshot() {
    items.set(List.copyOf(ipToItem.values()));
  }
}
