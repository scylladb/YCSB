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

import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link DnsRoundRobinPool}.
 *
 * <p>All tests use injected lambdas for DNS resolution and item creation — no real network
 * calls, no real {@code DynamoDbAsyncClient} instances. The pool's type parameter is
 * {@code String} (the raw IP address) which makes assertions trivial.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Even distribution of {@link DnsRoundRobinPool#next()} across N items</li>
 *   <li>Exact distribution under concurrent load (lock-free {@code AtomicInteger} counter)</li>
 *   <li>Discovery when DNS returns all IPs in a single query (common case)</li>
 *   <li>Discovery when DNS rotates one IP per query (strict round-robin DNS)</li>
 *   <li>Refresh adds a new IP and it enters the rotation immediately</li>
 *   <li>Refresh is a no-op when nothing changed (no allocation, no item creation)</li>
 *   <li>Constructor throws fast when DNS returns no addresses</li>
 * </ul>
 */
public class DnsRoundRobinPoolTest {

  private static final URI SEED = URI.create("http://scylla.cluster:8000");

  // ---- Helpers --------------------------------------------------------------

  /**
   * Creates a pool whose items ARE the IP strings. This lets us assert exactly
   * which "client" (IP) was selected on each {@code next()} call.
   */
  private static DnsRoundRobinPool<String> pool(DnsRoundRobinPool.DnsResolver resolver,
                                                int stableRounds) {
    // refreshIntervalSeconds=Long.MAX_VALUE: scheduler never fires during tests.
    // discoveryDelayMs=0: discovery loop completes instantly.
    return new DnsRoundRobinPool<>(SEED, uri -> uri.getHost(), resolver,
        Long.MAX_VALUE, stableRounds, 0);
  }

  /** Counts how many times each value was returned across {@code total} calls to {@code next()}. */
  private static Map<String, Integer> countDistribution(DnsRoundRobinPool<String> p, int total) {
    var counts = new HashMap<String, Integer>();
    for (int i = 0; i < total; i++) {
      counts.merge(p.next(), 1, Integer::sum);
    }
    return counts;
  }

  // ---- Distribution ---------------------------------------------------------

  /**
   * With 3 IPs and 300 requests the distribution must be exactly 100 each.
   * {@code Math.floorMod(counter, 3)} partitions [0..299] into three equal classes,
   * regardless of how threads interleave — no approximation, no tolerance.
   */
  @Test
  public void singleThreadEvenDistributionAcrossThreeNodes() {
    var p = pool(h -> List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), 2);

    var counts = countDistribution(p, 300);

    assertThat(counts.keySet(), hasSize(3));
    assertThat(counts.get("10.0.0.1"), is(100));
    assertThat(counts.get("10.0.0.2"), is(100));
    assertThat(counts.get("10.0.0.3"), is(100));

    p.close();
  }

  /** Same guarantee with a single IP: every request goes to it. */
  @Test
  public void singleNodeReceivesAllRequests() {
    var p = pool(h -> List.of("10.0.0.1"), 2);

    var counts = countDistribution(p, 100);

    assertThat(counts.keySet(), hasSize(1));
    assertThat(counts.get("10.0.0.1"), is(100));

    p.close();
  }

  /** Strict sequential ordering: IP1, IP2, IP3, IP1, IP2, IP3, … */
  @Test
  public void nextRotatesInDiscoveryOrder() {
    var p = pool(h -> List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), 2);

    assertThat(p.next(), is("10.0.0.1"));
    assertThat(p.next(), is("10.0.0.2"));
    assertThat(p.next(), is("10.0.0.3"));
    assertThat(p.next(), is("10.0.0.1")); // wraps

    p.close();
  }

  // ---- Concurrency ----------------------------------------------------------

  /**
   * Ten threads each make 300 calls concurrently (3 000 total, 3 IPs).
   * The distribution must be exactly 1 000 each — the {@code AtomicInteger} counter
   * assigns each call a unique index in [0..2999] and {@code floorMod(idx, 3)} is
   * a bijection that maps exactly 1 000 indices to each IP, regardless of scheduling.
   */
  @Test
  public void concurrentCallsDistributeExactly() throws InterruptedException {
    var p = pool(h -> List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), 2);

    int threads = 10;
    int callsPerThread = 300;
    var counts = new ConcurrentHashMap<String, AtomicInteger>();
    var latch = new CountDownLatch(threads);

    for (int t = 0; t < threads; t++) {
      new Thread(() -> {
        for (int i = 0; i < callsPerThread; i++) {
          counts.computeIfAbsent(p.next(), k -> new AtomicInteger()).incrementAndGet();
        }
        latch.countDown();
      }).start();
    }

    latch.await();

    int total = threads * callsPerThread;
    int expected = total / 3;

    assertThat(counts.keySet(), hasSize(3));
    assertThat(counts.get("10.0.0.1").get(), is(expected));
    assertThat(counts.get("10.0.0.2").get(), is(expected));
    assertThat(counts.get("10.0.0.3").get(), is(expected));

    p.close();
  }

  // ---- Discovery: all IPs at once -------------------------------------------

  /**
   * Most DNS servers return all A records in a single response.
   * Discovery should converge immediately and the pool should contain all three IPs.
   */
  @Test
  public void discoveryConvergesWhenDnsReturnsAllIpsAtOnce() {
    var p = pool(h -> List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), 3);

    assertThat(p.currentIps(), containsInAnyOrder("10.0.0.1", "10.0.0.2", "10.0.0.3"));
    assertThat(p.size(), is(3));

    p.close();
  }

  // ---- Discovery: one IP per query (strict DNS round-robin) -----------------

  /**
   * Some DNS servers rotate: query 1 returns IP1, query 2 returns IP2, etc.
   * The discovery loop must keep querying until no new IPs have appeared for
   * {@code stableRounds} consecutive rounds.
   *
   * <p>With 3 IPs and stableRounds=3 the loop will run until it sees 3 consecutive
   * empty rounds after collecting all IPs. We verify that all three IPs are found.
   */
  @Test
  public void discoveryAccumulatesIpsFromRotatingDns() {
    var ips = new String[]{"10.0.0.1", "10.0.0.2", "10.0.0.3"};
    var queryIndex = new AtomicInteger(0);
    // Each call returns the next IP in the rotation
    DnsRoundRobinPool.DnsResolver rotatingDns =
        h -> List.of(ips[queryIndex.getAndIncrement() % ips.length]);

    var p = pool(rotatingDns, 3);

    assertThat(p.currentIps(), containsInAnyOrder("10.0.0.1", "10.0.0.2", "10.0.0.3"));
    assertThat(p.size(), is(3));

    p.close();
  }

  /** Discovery stops early when fewer distinct IPs exist than stableRounds would allow. */
  @Test
  public void discoveryWithTwoIpsAndHighStableRounds() {
    // DNS always returns the same two IPs
    var p = pool(h -> List.of("10.0.0.1", "10.0.0.2"), 5);

    assertThat(p.currentIps(), containsInAnyOrder("10.0.0.1", "10.0.0.2"));
    assertThat(p.size(), is(2));

    p.close();
  }

  // ---- Refresh --------------------------------------------------------------

  /**
   * When a new IP appears in DNS after startup, calling {@code refresh()} must add it
   * to the pool and make it reachable via {@code next()}.
   */
  @Test
  public void refreshAddsNewIp() {
    var resolvedIps = new ArrayList<>(List.of("10.0.0.1", "10.0.0.2"));
    var p = pool(h -> new ArrayList<>(resolvedIps), 2);

    assertThat(p.size(), is(2));
    assertThat(p.currentIps(), containsInAnyOrder("10.0.0.1", "10.0.0.2"));

    // New node joins
    resolvedIps.add("10.0.0.3");
    p.refresh();

    assertThat(p.size(), is(3));
    assertThat(p.currentIps(), containsInAnyOrder("10.0.0.1", "10.0.0.2", "10.0.0.3"));

    // New IP must appear in round-robin
    var counts = countDistribution(p, 300);
    assertThat(counts.get("10.0.0.3"), is(100));

    p.close();
  }

  /**
   * When DNS returns the same IPs as before, {@code refresh()} must not invoke the
   * factory (no new item is created) and the pool size must stay the same.
   */
  @Test
  public void refreshIsNoOpWhenNothingChanged() {
    var factoryCalls = new AtomicInteger(0);
    DnsRoundRobinPool.DnsResolver resolver = h -> List.of("10.0.0.1", "10.0.0.2");

    // Factory counts invocations; initial discovery creates 2 items
    var p = new DnsRoundRobinPool<>(SEED,
        uri -> { factoryCalls.incrementAndGet(); return uri.getHost(); },
        resolver, Long.MAX_VALUE, 2, 0);

    int callsAfterDiscovery = factoryCalls.get();
    assertThat(callsAfterDiscovery, is(2)); // one per IP

    // Refresh: DNS unchanged → factory must NOT be called again
    p.refresh();

    assertThat(factoryCalls.get(), is(callsAfterDiscovery));
    assertThat(p.size(), is(2));

    p.close();
  }

  /**
   * Refreshing multiple times adds each new IP exactly once, even if the same
   * new IP appears in several consecutive DNS responses.
   */
  @Test
  public void refreshAddsEachNewIpOnlyOnce() {
    var resolvedIps = new ArrayList<>(List.of("10.0.0.1"));
    var p = pool(h -> new ArrayList<>(resolvedIps), 2);

    assertThat(p.size(), is(1));

    // Add new IP; refresh twice — pool should still have exactly 2 items
    resolvedIps.add("10.0.0.2");
    p.refresh();
    p.refresh();

    assertThat(p.size(), is(2));
    assertThat(p.currentIps(), containsInAnyOrder("10.0.0.1", "10.0.0.2"));

    p.close();
  }

  // ---- Fail-fast ------------------------------------------------------------

  /**
   * If DNS returns no addresses during discovery, the constructor must throw
   * {@link IllegalStateException} immediately so misconfigured runs fail fast
   * rather than producing silent incorrect results.
   */
  @Test(expected = IllegalStateException.class)
  public void constructorThrowsWhenDnsReturnsNothing() {
    pool(h -> List.of(), 2);
  }

  // ---- Pool size ------------------------------------------------------------

  @Test
  public void sizeReflectsCurrentPoolSize() {
    var p = pool(h -> List.of("10.0.0.1", "10.0.0.2", "10.0.0.3"), 2);
    assertThat(p.size(), is(3));
    p.close();
  }
}
