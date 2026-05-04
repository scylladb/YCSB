/*
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package site.ycsb.db.dynamodb;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Test-only {@link InetAddressResolverProvider} that intercepts a configured hostname and
 * returns one IP per call from a configured rotation, simulating round-robin DNS. Any other
 * hostname is delegated to the JVM's built-in resolver, so tests that don't configure this
 * provider see normal DNS behavior.
 *
 * <p>The provider is loaded once via the {@code java.net.spi.InetAddressResolverProvider}
 * SPI on first use of {@link InetAddress}. Tests configure rotation via
 * {@link #configure(String, List)} and reset state between cases via {@link #clear()}.
 *
 * <p>Per-IP resolution counts are tracked for diagnostic assertions.
 */
public final class TestRoundRobinResolverProvider extends InetAddressResolverProvider {

  // ---- Static configuration (modified by tests, read by lookup callbacks) ---

  private static final AtomicReference<String> hostname = new AtomicReference<>("");
  private static final AtomicReference<List<InetAddress>> rotation =
      new AtomicReference<>(List.of());
  private static final AtomicInteger counter = new AtomicInteger();
  private static final Map<String, AtomicInteger> perIpCounts = new ConcurrentHashMap<>();

  /**
   * Configures the rotation. After this call, lookups for {@code host} will return one IP per
   * call in round-robin order; lookups for any other hostname delegate to the built-in resolver.
   *
   * @param host the hostname to intercept; pass an empty string to disable interception
   * @param ips  the IP literals (no DNS lookup performed) to rotate through
   */
  public static void configure(String host, List<String> ips) throws UnknownHostException {
    var addrs = new ArrayList<InetAddress>(ips.size());
    for (var ip : ips) {
      addrs.add(InetAddress.getByName(ip)); // IP literal — never triggers a DNS query
    }
    rotation.set(List.copyOf(addrs));
    hostname.set(host == null ? "" : host);
    counter.set(0);
    perIpCounts.clear();
    for (var ip : ips) {
      perIpCounts.put(ip, new AtomicInteger());
    }
  }

  /** Disables interception; subsequent lookups always delegate to the built-in resolver. */
  public static void clear() {
    hostname.set("");
    rotation.set(List.of());
    counter.set(0);
    perIpCounts.clear();
  }

  /** Returns total intercepted lookups since the last {@link #configure}. */
  public static int totalLookups() {
    return counter.get();
  }

  /** Returns a snapshot of per-IP intercepted lookup counts. */
  public static Map<String, Integer> perIpLookups() {
    var snapshot = new HashMap<String, Integer>();
    perIpCounts.forEach((ip, ai) -> snapshot.put(ip, ai.get()));
    return snapshot;
  }

  // ---- SPI hooks ------------------------------------------------------------

  @Override
  public InetAddressResolver get(Configuration cfg) {
    return rotatingResolver(cfg.builtinResolver());
  }

  @Override
  public String name() {
    return "ycsb-test-round-robin";
  }

  /**
   * Creates a resolver with the same rotation logic as the SPI hook but with an explicit
   * fallback. Visible for tests so they can exercise rotation without an SPI
   * {@link Configuration} (sealed in the JDK and not implementable from outside).
   */
  static InetAddressResolver rotatingResolver(InetAddressResolver fallback) {
    return new InetAddressResolver() {
      @Override
      public Stream<InetAddress> lookupByName(String host, LookupPolicy policy)
          throws UnknownHostException {
        var target = hostname.get();
        if (!target.isEmpty() && target.equalsIgnoreCase(host)) {
          var current = rotation.get();
          if (!current.isEmpty()) {
            int idx = Math.floorMod(counter.getAndIncrement(), current.size());
            var picked = current.get(idx);
            var ai = perIpCounts.get(picked.getHostAddress());
            if (ai != null) {
              ai.incrementAndGet();
            }
            return Stream.of(picked);
          }
        }
        return fallback.lookupByName(host, policy);
      }

      @Override
      public String lookupByAddress(byte[] addr) throws UnknownHostException {
        return fallback.lookupByAddress(addr);
      }
    };
  }
}
