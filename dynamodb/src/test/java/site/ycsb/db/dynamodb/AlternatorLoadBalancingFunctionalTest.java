/*
 * Copyright (c) 2026 YCSB contributors.
 */
package site.ycsb.db.dynamodb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.DynamoDBClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Checks that the binding spreads requests across every node that {@code /localnodes}
 * reports. With the binding defaults, probes promote each discovered node out of
 * quarantine within seconds.
 */
public class AlternatorLoadBalancingFunctionalTest {

  private static final String SEED = "127.0.0.1";
  private static final String PEER = "127.0.0.2";
  private static final String TABLE = "usertable";

  private MockAlternatorCluster cluster;
  private DynamoDBClient client;

  @Before
  public void setUp() throws Exception {
    cluster = new MockAlternatorCluster(SEED, PEER).start();

    Properties props = new Properties();
    props.setProperty("dynamodb.endpoint", "http://" + SEED + ":" + cluster.port());
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.accessKey", "test");
    props.setProperty("dynamodb.secretKey", "test");
    props.setProperty("dynamodb.primaryKey", "y_id");
    props.setProperty("dynamodb.alternator.loadbalancing", "true");

    client = new DynamoDBClient();
    client.setProperties(props);
    client.init();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (client != null) {
        client.cleanup();
      }
    } finally {
      if (cluster != null) {
        cluster.close();
      }
    }
  }

  @Test
  public void spreadsRequestsAcrossDiscoveredNodes() throws Exception {
    MockAlternatorCluster.await(() -> cluster.localNodesServed() > 0, 10_000,
        () -> "the client never called /localnodes");
    AtomicInteger warmUp = new AtomicInteger();
    MockAlternatorCluster.await(() -> {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("f", new StringByteIterator("warm"));
      assertThat(client.insert(TABLE, "warm" + warmUp.getAndIncrement(), values), is(Status.OK));
      return cluster.requests().stream().anyMatch(r -> r.node().equals(PEER));
    }, 30_000, () -> "no request reached the discovered peer: " + cluster.requests());
    int firstRequest = cluster.requestCount();

    for (int i = 0; i < 40; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("f", new StringByteIterator("v" + i));
      assertThat("insert k" + i, client.insert(TABLE, "k" + i, values), is(Status.OK));
    }
    for (int i = 0; i < 10; i++) {
      Map<String, ByteIterator> result = new HashMap<>();
      assertThat("read k" + i, client.read(TABLE, "k" + i, null, result), is(Status.OK));
      assertThat(result.get("f").toString(), is("v" + i));
    }

    List<MockAlternatorCluster.Request> requests = cluster.requestsSince(firstRequest);
    Set<String> nodes = requests.stream()
        .map(MockAlternatorCluster.Request::node)
        .collect(Collectors.toSet());
    assertThat("nodes that received requests: " + requests, nodes, is(Set.of(SEED, PEER)));
    assertThat(cluster.errorResponses(), is(0));
  }
}
