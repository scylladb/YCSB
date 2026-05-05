/*
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package site.ycsb.db.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.DynamoDBClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

/**
 * Boots three independent ScyllaDB Alternator containers on a Docker bridge network,
 * installs a per-process round-robin DNS resolver that rotates a fake hostname over the
 * three container bridge IPs, and asserts that the YCSB binding distributes requests
 * roughly evenly across the three nodes.
 *
 * <p>This test is the full-stack version of the v0.2.0 / v1.1.0 / v1.2.0 design: a single
 * client, a hostname endpoint, and DNS round-robin handing connections to different IPs.
 *
 * <p><b>Linux only.</b> The test reaches each Scylla via its Docker bridge IP at the
 * container-internal port 8000 (and Prometheus at 9180). On Linux the bridge is
 * host-routable; on macOS / Windows Docker Desktop it is not, so the test is skipped.
 */
public class DnsRoundRobinDistributionTest {

  static {
    // Disable JVM positive DNS cache before any class triggers a lookup. Required for the
    // SPI to be invoked on every new connection, not just the first one.
    java.security.Security.setProperty("networkaddress.cache.ttl", "0");
    java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
  }

  private static final String FAKE_HOST = "scylla-cluster.test";
  private static final int ALTERNATOR_PORT = 8000;
  private static final int PROMETHEUS_PORT = 9180;
  private static final String TABLE = "usertable_rr";
  private static final String PRIMARY_KEY = "y_id";

  private static Network network;
  private static GenericContainer<?> scylla1;
  private static GenericContainer<?> scylla2;
  private static GenericContainer<?> scylla3;
  private static List<GenericContainer<?>> nodes;
  private static List<String> containerIps;

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  // ---- Lifecycle ------------------------------------------------------------

  @BeforeClass
  public static void start() throws Exception {
    Assume.assumeTrue("Linux only — relies on host-routable Docker bridge IPs",
        System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux"));
    Assume.assumeTrue("Docker is not available", isDockerAvailable());

    final String version = System.getProperty("scylla.version", "2026.1");
    final String imageProp = System.getProperty("scylla.image");
    final String image = (imageProp != null && !imageProp.isEmpty())
        ? imageProp
        : ("scylladb/scylla:" + version);
    var dockerImage = DockerImageName.parse(image);

    network = Network.newNetwork();
    scylla1 = createScylla(dockerImage, network, "rr-scylla-1");
    scylla2 = createScylla(dockerImage, network, "rr-scylla-2");
    scylla3 = createScylla(dockerImage, network, "rr-scylla-3");
    nodes = List.of(scylla1, scylla2, scylla3);

    Startables.deepStart(nodes).join();

    containerIps = new ArrayList<>();
    for (var c : nodes) {
      containerIps.add(bridgeIp(c));
    }

    // Configure the test's round-robin DNS resolver to map FAKE_HOST -> the 3 container IPs.
    TestRoundRobinResolverProvider.configure(FAKE_HOST, containerIps);

    // Sanity: each node's Alternator must be reachable on its bridge IP at port 8000.
    for (int i = 0; i < nodes.size(); i++) {
      waitForAlternator(containerIps.get(i), ALTERNATOR_PORT, Duration.ofMinutes(2));
    }

    // Create the table on each node independently — these are 3 isolated single-node
    // clusters; the test only cares about request counts per node, not data replication.
    for (var c : nodes) {
      createTableViaMappedPort(c);
    }
  }

  @AfterClass
  public static void stop() {
    TestRoundRobinResolverProvider.clear();
    if (nodes != null) {
      for (var c : nodes) {
        try {
          c.stop();
        } catch (Exception ignore) {
          // best effort
        }
      }
    }
    if (network != null) {
      try {
        network.close();
      } catch (Exception ignore) {
        // best effort
      }
    }
  }

  // ---- The actual test ------------------------------------------------------

  @Test
  public void requestsAreRoughlyEvenAcrossThreeNodes() throws Exception {
    // 30 worker threads → 30 Netty connections → 10 per IP (exactly divisible by 3).
    // 3000 requests gives ~100 requests per connection so noise from per-connection
    // perf jitter averages out. Test runtime is ~30s.
    final int requests = 3000;
    final int threads = 30;

    // Baseline counts before workload.
    var before = new ArrayList<Long>();
    for (var c : nodes) {
      before.add(scrapeCoordinatorWriteCount(c));
    }

    // Run the workload through the YCSB binding using the fake hostname.
    var props = new Properties();
    props.setProperty("dynamodb.endpoint", "http://" + FAKE_HOST + ":" + ALTERNATOR_PORT);
    props.setProperty("dynamodb.primaryKey", PRIMARY_KEY);
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.awsAccessKey", "dummy");
    props.setProperty("dynamodb.awsSecretKey", "dummy");
    props.setProperty(Client.THREAD_COUNT_PROPERTY, String.valueOf(threads));

    var client = new DynamoDBClient();
    client.setProperties(props);
    client.init();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      // Issue inserts concurrently — sequential calls would let Netty serve everything on
      // a single keep-alive connection, defeating connection-pool DNS rotation.
      var futures = new ArrayList<Future<Status>>(requests);
      for (int i = 0; i < requests; i++) {
        final int idx = i;
        futures.add(pool.submit(() -> {
          var values = new HashMap<String, ByteIterator>();
          values.put("f0", new StringByteIterator("v" + idx));
          values.put("f1", new StringByteIterator("w" + idx));
          return client.insert(TABLE, "k" + idx, values);
        }));
      }
      for (int i = 0; i < futures.size(); i++) {
        Status s = futures.get(i).get(60, TimeUnit.SECONDS);
        assertEquals("insert " + i + " failed: " + s, Status.OK, s);
      }
    } finally {
      pool.shutdown();
      pool.awaitTermination(30, TimeUnit.SECONDS);
      client.cleanup();
    }

    // Compute deltas and assert balance.
    var deltas = new ArrayList<Long>();
    for (int i = 0; i < nodes.size(); i++) {
      long after = scrapeCoordinatorWriteCount(nodes.get(i));
      deltas.add(after - before.get(i));
    }

    long total = deltas.stream().mapToLong(Long::longValue).sum();
    System.out.println("DNS round-robin IT — per-node coordinator-write deltas: " + deltas
        + ", per-IP DNS rotations: " + TestRoundRobinResolverProvider.perIpLookups()
        + ", total intercepted lookups: " + TestRoundRobinResolverProvider.totalLookups());
    assertTrue("expected at least " + requests + " coordinator writes total but got " + total
        + " (per-node deltas: " + deltas + "). The metric name probed may not exist in this "
        + "Scylla version — see scrapeCoordinatorWriteCount().", total >= requests);

    double mean = total / 3.0;
    double tolerance = 0.15 * mean; // ±15%
    for (int i = 0; i < deltas.size(); i++) {
      long d = deltas.get(i);
      assertTrue("node " + (i + 1) + " received " + d + " requests; expected within "
          + (long) tolerance + " of mean " + (long) mean + ". Per-node deltas: " + deltas
          + ", per-IP DNS rotations: " + TestRoundRobinResolverProvider.perIpLookups(),
          Math.abs(d - mean) <= tolerance);
    }

    // Bonus: also assert the SPI rotated approximately evenly at the connection-open layer.
    Map<String, Integer> rotation = TestRoundRobinResolverProvider.perIpLookups();
    int rotationTotal = rotation.values().stream().mapToInt(Integer::intValue).sum();
    assertTrue("expected the SPI to be invoked at least once per IP — got " + rotation,
        rotationTotal >= 3);
    for (var ip : containerIps) {
      Integer n = rotation.get(ip);
      assertTrue("IP " + ip + " was never selected by the SPI (rotation=" + rotation + ")",
          n != null && n > 0);
    }
  }

  // ---- Helpers --------------------------------------------------------------

  private static GenericContainer<?> createScylla(DockerImageName image, Network net, String alias) {
    GenericContainer<?> c = new GenericContainer<>(image)
        .withNetwork(net)
        .withNetworkAliases(alias)
        .withExposedPorts(ALTERNATOR_PORT, PROMETHEUS_PORT)
        .withCommand(
            "--alternator-port=" + ALTERNATOR_PORT,
            "--alternator-write-isolation=always",
            "--smp=1",
            "--memory=750M",
            "--overprovisioned=1",
            "--developer-mode=1")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
    return c;
  }

  private static String bridgeIp(GenericContainer<?> c) {
    var nets = c.getContainerInfo().getNetworkSettings().getNetworks();
    if (nets.isEmpty()) {
      throw new IllegalStateException("container has no networks");
    }
    // The container is attached to a single user-defined network; first entry is ours.
    var ip = nets.values().iterator().next().getIpAddress();
    if (ip == null || ip.isEmpty()) {
      throw new IllegalStateException("container has no bridge IP — IPv6-only?");
    }
    return ip;
  }

  private static void waitForAlternator(String ip, int port, Duration timeout) throws Exception {
    var deadline = System.nanoTime() + timeout.toNanos();
    Exception last = null;
    while (System.nanoTime() < deadline) {
      try (var sock = new java.net.Socket()) {
        sock.connect(new java.net.InetSocketAddress(ip, port), 1000);
        return;
      } catch (Exception e) {
        last = e;
        Thread.sleep(500);
      }
    }
    throw new IllegalStateException("Alternator not reachable at " + ip + ":" + port, last);
  }

  private static void createTableViaMappedPort(GenericContainer<?> c) throws Exception {
    var ddb = DynamoDbClient.builder()
        .endpointOverride(URI.create("http://" + c.getHost() + ":" + c.getMappedPort(ALTERNATOR_PORT)))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("dummy", "dummy")))
        .build();
    try {
      try {
        ddb.createTable(CreateTableRequest.builder()
            .tableName(TABLE)
            .keySchema(KeySchemaElement.builder().attributeName(PRIMARY_KEY).keyType(KeyType.HASH).build())
            .attributeDefinitions(AttributeDefinition.builder()
                .attributeName(PRIMARY_KEY).attributeType(ScalarAttributeType.S).build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build());
      } catch (ResourceInUseException ignored) {
        // already exists
      }
      var deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
      while (System.nanoTime() < deadline) {
        var status = ddb.describeTable(DescribeTableRequest.builder().tableName(TABLE).build())
            .table().tableStatus();
        if (status == TableStatus.ACTIVE) {
          return;
        }
        Thread.sleep(500);
      }
      fail("table never became ACTIVE on " + c.getHost() + ":" + c.getMappedPort(ALTERNATOR_PORT));
    } finally {
      ddb.close();
    }
  }

  /**
   * Sums per-shard counters that count <em>coordinator</em>-level write activity on this
   * Scylla node. Tries the Alternator-specific operation counter first, then falls back to
   * the storage-proxy counter that exists in every Scylla version. Returns 0 if the node's
   * Prometheus endpoint is reachable but neither metric family is present (fail-safe; the
   * test will then fail the total-requests assertion with a clear diagnostic).
   */
  private static long scrapeCoordinatorWriteCount(GenericContainer<?> c) throws Exception {
    var url = URI.create("http://" + c.getHost() + ":" + c.getMappedPort(PROMETHEUS_PORT) + "/metrics");
    var req = HttpRequest.newBuilder(url)
        .timeout(Duration.ofSeconds(10))
        .GET()
        .build();
    var resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    if (resp.statusCode() != 200) {
      return 0L;
    }

    String body = resp.body();

    // Preferred: Alternator-level operation counter (PutItem/UpdateItem/etc).
    long alternatorOps = sumMatching(body,
        Pattern.compile("^scylla_alternator_(operation|total_operations|requests_total)\\b.*?\\s+([0-9eE.+-]+)\\s*$",
            Pattern.MULTILINE));
    if (alternatorOps > 0) {
      return alternatorOps;
    }

    // Fallback: coordinator-level writes from storage proxy. Each Alternator put_item
    // becomes a coordinator write; this metric exists across many Scylla versions.
    return sumMatching(body,
        Pattern.compile("^scylla_storage_proxy_coordinator_completed_writes\\b.*?\\s+([0-9eE.+-]+)\\s*$",
            Pattern.MULTILINE));
  }

  private static long sumMatching(String text, Pattern p) {
    Matcher m = p.matcher(text);
    long total = 0;
    while (m.find()) {
      try {
        // Last capture group is always the numeric value.
        String numeric = m.group(m.groupCount());
        double v = Double.parseDouble(numeric);
        total += (long) v;
      } catch (NumberFormatException ignore) {
        // skip non-numeric lines
      }
    }
    return total;
  }

  // ---- Docker availability shim (mirrors DynamoDBClientTest) ----------------

  private static boolean isDockerAvailable() {
    try {
      configureDockerHost();
      org.testcontainers.DockerClientFactory.instance().client();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static void configureDockerHost() {
    if (System.getenv("DOCKER_HOST") != null || System.getProperty("docker.host") != null) {
      return;
    }
    var home = System.getProperty("user.home");
    if (home == null || home.isEmpty()) {
      return;
    }
    var desktopSocket = Path.of(home, ".docker", "run", "docker.sock");
    if (Files.exists(desktopSocket)) {
      System.setProperty("docker.host", "unix://" + desktopSocket);
    }
  }

}
