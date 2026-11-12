/*
 * Copyright (c) 2026 YCSB contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Reproducer for SCT-370. During a rolling upgrade the Alternator seed node went down, and
 * YCSB returned {@code CLIENT_ERROR (connection refused)} for 62,624 UPDATE operations,
 * although every other node was healthy.
 *
 * <p>The binding gives the Alternator client one seed URI. The client polls
 * {@code /localnodes} to discover the other nodes. When the seed dies, the client must stop
 * sending requests to it and use the nodes that are still up.
 *
 * <p>Node health routing holds discovered nodes in quarantine until a probe promotes them.
 * With the binding defaults, the peer becomes active during warm-up. The client cannot know that the seed is dead until a request to it
 * fails. After {@code FAILURE_THRESHOLD} consecutive failures, node health routing marks the
 * seed down. The test makes sure that the client sends no more requests to the seed after
 * that point, and that SDK retries move every failed attempt to the peer.
 */
public class AlternatorSeedFailoverFunctionalTest {

  private static final String SEED = "127.0.0.1";
  private static final String PEER = "127.0.0.2";
  private static final String TABLE = "usertable";
  private static final int KEYS = 20;
  private static final int UPDATES = 200;
  private static final int FAILURE_THRESHOLD = 1;

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
    props.setProperty("dynamodb.alternator.health.failureThreshold",
        String.valueOf(FAILURE_THRESHOLD));

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
  public void seedDeathDoesNotKillAllTraffic() throws Exception {
    AtomicInteger insertsDone = new AtomicInteger();
    MockAlternatorCluster.await(() -> {
      int i = insertsDone.getAndIncrement();
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("f", new StringByteIterator("warm-" + i));
      assertThat("warm-up insert " + i, client.insert(TABLE, "k" + (i % KEYS), values), is(Status.OK));
      return reachedBothNodes(cluster.requests());
    }, 30_000, () -> "warm-up traffic never reached both nodes: " + cluster.requests());

    cluster.markDown(SEED);

    int firstUpdate = cluster.requestCount();
    int failed = 0;
    for (int i = 0; i < UPDATES; i++) {
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("f", new StringByteIterator("post-kill-" + i));
      if (client.update(TABLE, "k" + (i % KEYS), values) != Status.OK) {
        failed++;
      }
    }

    List<MockAlternatorCluster.Request> afterKill = cluster.requestsSince(firstUpdate);
    long seedAttempts = afterKill.stream()
        .filter(r -> r.operation().equals("UpdateItem") && r.node().equals(SEED))
        .count();

    assertThat("error responses from nodes that are up", cluster.errorResponses(), is(0));
    assertThat("failed updates after the seed died", failed, is(0));
    assertThat("UpdateItem attempts sent to the dead seed, retries included: " + afterKill,
        seedAttempts <= FAILURE_THRESHOLD, is(true));
  }

  private static boolean reachedBothNodes(List<MockAlternatorCluster.Request> requests) {
    return requests.stream().anyMatch(r -> r.node().equals(SEED))
        && requests.stream().anyMatch(r -> r.node().equals(PEER));
  }
}
