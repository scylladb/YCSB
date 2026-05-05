/*
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package site.ycsb.db.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolver.LookupPolicy;
import java.net.spi.InetAddressResolverProvider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Test;

/**
 * Verifies the round-robin {@link InetAddressResolverProvider} rotates IPs as configured
 * and is registered via the SPI.
 *
 * <p>Most tests use the package-private
 * {@code TestRoundRobinResolverProvider#rotatingResolver(InetAddressResolver)} factory so
 * we can pass our own fallback. That bypasses the JVM's positive-DNS cache, which would
 * otherwise hide repeated resolutions of the same hostname from the resolver. One
 * end-to-end test uses {@link InetAddress#getByName(String)} with a fresh hostname per
 * assertion to confirm the SPI is loaded by the JVM.
 */
public class TestRoundRobinResolverProviderTest {

  /** Stub fallback that always throws — proves delegation paths flow into it. */
  private static final InetAddressResolver THROWING_FALLBACK = new InetAddressResolver() {
    @Override
    public Stream<InetAddress> lookupByName(String host, LookupPolicy policy)
        throws UnknownHostException {
      throw new UnknownHostException(host);
    }

    @Override
    public String lookupByAddress(byte[] addr) throws UnknownHostException {
      throw new UnknownHostException();
    }
  };

  @After
  public void clear() {
    TestRoundRobinResolverProvider.clear();
  }

  // ---- Direct-API tests (cache-free) ----------------------------------------

  @Test
  public void rotatesAcrossConfiguredIps() throws Exception {
    var ips = List.of("10.0.0.1", "10.0.0.2", "10.0.0.3");
    TestRoundRobinResolverProvider.configure("rr-host.test", ips);

    var resolver = TestRoundRobinResolverProvider.rotatingResolver(THROWING_FALLBACK);
    var seq = new ArrayList<String>();
    for (int i = 0; i < 9; i++) {
      var addr = resolver.lookupByName("rr-host.test", LookupPolicy.of(LookupPolicy.IPV4))
          .findFirst().orElseThrow();
      seq.add(addr.getHostAddress());
    }

    assertEquals("strict round-robin order across 9 lookups",
        List.of("10.0.0.1", "10.0.0.2", "10.0.0.3",
                "10.0.0.1", "10.0.0.2", "10.0.0.3",
                "10.0.0.1", "10.0.0.2", "10.0.0.3"),
        seq);
    assertEquals(9, TestRoundRobinResolverProvider.totalLookups());

    Map<String, Integer> perIp = TestRoundRobinResolverProvider.perIpLookups();
    var expected = new HashMap<String, Integer>();
    for (var ip : ips) {
      expected.put(ip, 3);
    }
    assertEquals(expected, perIp);
  }

  @Test
  public void hostnameMatchIsCaseInsensitive() throws Exception {
    TestRoundRobinResolverProvider.configure("Rr-Host.Test", List.of("10.0.0.1", "10.0.0.2"));
    var resolver = TestRoundRobinResolverProvider.rotatingResolver(THROWING_FALLBACK);

    var a = resolver.lookupByName("RR-HOST.TEST", LookupPolicy.of(LookupPolicy.IPV4))
        .findFirst().orElseThrow().getHostAddress();
    var b = resolver.lookupByName("rr-host.test", LookupPolicy.of(LookupPolicy.IPV4))
        .findFirst().orElseThrow().getHostAddress();

    assertEquals("10.0.0.1", a);
    assertEquals("10.0.0.2", b);
  }

  @Test
  public void unrecognizedHostnamesGoToFallback() throws Exception {
    TestRoundRobinResolverProvider.configure("rr-host.test", List.of("10.0.0.1"));
    var resolver = TestRoundRobinResolverProvider.rotatingResolver(THROWING_FALLBACK);

    try {
      resolver.lookupByName("not-our-host.test", LookupPolicy.of(LookupPolicy.IPV4)).findFirst();
      fail("Expected UnknownHostException from delegated lookup");
    } catch (UnknownHostException expected) {
      assertTrue("fallback received the right hostname",
          expected.getMessage().contains("not-our-host.test"));
    }
    assertEquals("intercept counter must not advance for non-matching hosts",
        0, TestRoundRobinResolverProvider.totalLookups());
  }

  @Test
  public void clearStopsRotation() throws Exception {
    TestRoundRobinResolverProvider.configure("rr-host.test", List.of("10.0.0.1", "10.0.0.2"));
    var resolver = TestRoundRobinResolverProvider.rotatingResolver(THROWING_FALLBACK);

    resolver.lookupByName("rr-host.test", LookupPolicy.of(LookupPolicy.IPV4))
        .findFirst().orElseThrow();
    assertEquals(1, TestRoundRobinResolverProvider.totalLookups());

    TestRoundRobinResolverProvider.clear();
    assertEquals(0, TestRoundRobinResolverProvider.totalLookups());

    try {
      resolver.lookupByName("rr-host.test", LookupPolicy.of(LookupPolicy.IPV4)).findFirst();
      fail("Expected UnknownHostException after clear()");
    } catch (UnknownHostException expected) {
      // OK
    }
  }

  // ---- End-to-end SPI registration check ------------------------------------

  /**
   * Confirms the META-INF/services registration is picked up by the JVM. Uses a fresh
   * hostname so the JVM's positive-DNS cache can't hide the lookup from the resolver.
   */
  @Test
  public void spiIsRegisteredAndIntercepted() throws Exception {
    // Sanity check: ServiceLoader sees the provider.
    boolean spiPresent = ServiceLoader.load(InetAddressResolverProvider.class).stream()
        .anyMatch(p -> p.type() == TestRoundRobinResolverProvider.class);
    assertTrue("TestRoundRobinResolverProvider must be discoverable via ServiceLoader "
        + "— check META-INF/services entry", spiPresent);

    var uniqueHost = "ycsb-spi-check-" + System.nanoTime() + ".invalid";
    TestRoundRobinResolverProvider.configure(uniqueHost, List.of("10.0.0.42"));

    var addr = InetAddress.getByName(uniqueHost);
    assertNotNull(addr);
    assertEquals("10.0.0.42", addr.getHostAddress());
  }
}
