/*
 * Copyright (c) 2015-2016 YCSB contributors. All Rights Reserved.
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

package site.ycsb.db.dynamodb;

import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.DynamoDBClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.time.Duration;
import java.util.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class DynamoDBClientIntegrationTest {

  private static final String TABLE_NAME = "usertable";
  private static final String PRIMARY_KEY = "p";
  private static final int ALTERNATOR_PORT = 8000;
  private static final int REST_API_PORT = 10000;

  private static GenericContainer<?> scylla;
  private static DynamoDBClient ycsbClient;
  private static DynamoDbClient adminClient;
  private static String endpoint;
  private static String restApiEndpoint;

  @BeforeClass
  public static void startContainer() throws Exception {
    scylla = new GenericContainer<>(
        DockerImageName.parse("scylladb/scylla:2025.4"))
        .withExposedPorts(ALTERNATOR_PORT, REST_API_PORT)
        .withCommand("--alternator-port", String.valueOf(ALTERNATOR_PORT),
            "--alternator-write-isolation", "always")
        .waitingFor(Wait.forHttp("/localnodes")
            .forPort(ALTERNATOR_PORT)
            .withStartupTimeout(Duration.ofMinutes(2)));
    scylla.start();

    endpoint = "http://%s:%d".formatted(
        scylla.getHost(),
        scylla.getMappedPort(ALTERNATOR_PORT));

    restApiEndpoint = "http://%s:%d".formatted(
        scylla.getHost(),
        scylla.getMappedPort(REST_API_PORT));

    adminClient = DynamoDbClient.builder()
        .endpointOverride(URI.create(endpoint))
        .region(Region.US_EAST_1)
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("test", "test")))
        .build();

    createTableIfNotExists();

    // Wait for table to be fully ready
    Thread.sleep(1000);

    var props = new Properties();
    props.setProperty("dynamodb.endpoint", endpoint);
    props.setProperty("dynamodb.primaryKey", PRIMARY_KEY);
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.awsAccessKey", "test");
    props.setProperty("dynamodb.awsSecretKey", "test");

    ycsbClient = new DynamoDBClient();
    ycsbClient.setProperties(props);
    ycsbClient.init();
  }

  @AfterClass
  public static void stopContainer() {
    if (ycsbClient != null) {
      try {
        ycsbClient.cleanup();
      } catch (Exception e) {
        // Ignore
      }
    }
    if (adminClient != null) {
      try {
        adminClient.close();
      } catch (Exception e) {
        // Ignore
      }
    }
    if (scylla != null) {
      scylla.stop();
    }
  }

  private static void createTableIfNotExists() {
    try {
      adminClient.describeTable(DescribeTableRequest.builder()
          .tableName(TABLE_NAME)
          .build());
    } catch (ResourceNotFoundException e) {
      adminClient.createTable(CreateTableRequest.builder()
          .tableName(TABLE_NAME)
          .keySchema(KeySchemaElement.builder()
              .attributeName(PRIMARY_KEY)
              .keyType(KeyType.HASH)
              .build())
          .attributeDefinitions(AttributeDefinition.builder()
              .attributeName(PRIMARY_KEY)
              .attributeType(ScalarAttributeType.S)
              .build())
          .billingMode(BillingMode.PAY_PER_REQUEST)
          .build());

      adminClient.waiter().waitUntilTableExists(
          DescribeTableRequest.builder().tableName(TABLE_NAME).build());
    }
  }

  @Test
  public void insertAndReadSingleRecord() {
    var key = "test-key-" + UUID.randomUUID();
    var values = new HashMap<String, ByteIterator>();
    values.put("field1", new StringByteIterator("value1"));
    values.put("field2", new StringByteIterator("value2"));

    var insertStatus = ycsbClient.insert(TABLE_NAME, key, values);
    assertThat(insertStatus, is(Status.OK));

    var result = new HashMap<String, ByteIterator>();
    var readStatus = ycsbClient.read(TABLE_NAME, key, null, result);

    assertThat(readStatus, is(Status.OK));
    assertThat(result.get("field1").toString(), is("value1"));
    assertThat(result.get("field2").toString(), is("value2"));
  }

  @Test
  public void updateExistingRecord() {
    var key = "test-key-" + UUID.randomUUID();
    var initial = new HashMap<String, ByteIterator>();
    initial.put("field1", new StringByteIterator("initial"));

    ycsbClient.insert(TABLE_NAME, key, initial);

    var update = new HashMap<String, ByteIterator>();
    update.put("field1", new StringByteIterator("updated"));

    var updateStatus = ycsbClient.update(TABLE_NAME, key, update);
    assertThat(updateStatus, is(Status.OK));

    var result = new HashMap<String, ByteIterator>();
    ycsbClient.read(TABLE_NAME, key, null, result);

    assertThat(result.get("field1").toString(), is("updated"));
  }

  @Test
  public void deleteExistingRecord() {
    var key = "test-key-" + UUID.randomUUID();
    var values = new HashMap<String, ByteIterator>();
    values.put("field1", new StringByteIterator("value1"));

    ycsbClient.insert(TABLE_NAME, key, values);

    var deleteStatus = ycsbClient.delete(TABLE_NAME, key);
    assertThat(deleteStatus, is(Status.OK));

    var result = new HashMap<String, ByteIterator>();
    ycsbClient.read(TABLE_NAME, key, null, result);

    assertTrue(result.isEmpty() || result.get("field1") == null);
  }

  @Test
  public void scanReturnsMultipleRecords() {
    var prefix = "scan-test-" + UUID.randomUUID() + "-";
    for (int i = 0; i < 5; i++) {
      var values = new HashMap<String, ByteIterator>();
      values.put("field1", new StringByteIterator("value" + i));
      ycsbClient.insert(TABLE_NAME, prefix + i, values);
    }

    var results = new Vector<HashMap<String, ByteIterator>>();
    var scanStatus = ycsbClient.scan(TABLE_NAME, null, 10, null, results);

    assertThat(scanStatus, is(Status.OK));
    assertTrue(results.size() >= 5);
  }

  @Test
  public void loadBalancingModeWorks() {
    var client = getDynamoDBClient();
    try {
      client.init();
      var key = "internal-lb-test-" + UUID.randomUUID();
      var values = new HashMap<String, ByteIterator>();
      values.put("field1", new StringByteIterator("value1"));

      var insertStatus = client.insert(TABLE_NAME, key, values);
      assertThat(insertStatus, is(Status.OK));

      var result = new HashMap<String, ByteIterator>();
      var readStatus = client.read(TABLE_NAME, key, null, result);

      assertThat(readStatus, is(Status.OK));
      assertThat(result.get("field1").toString(), is("value1"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        client.cleanup();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  private static @NotNull DynamoDBClient getDynamoDBClient() {
    var props = new Properties();
    props.setProperty("dynamodb.endpoint", endpoint);
    props.setProperty("dynamodb.primaryKey", PRIMARY_KEY);
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.awsAccessKey", "test");
    props.setProperty("dynamodb.awsSecretKey", "test");
    props.setProperty("dynamodb.alternator.loadbalancing", "true");
    props.setProperty("dynamodb.alternator.usePackageLoadBalancer", "false");

    var client = new DynamoDBClient();
    client.setProperties(props);
    return client;
  }

  @Test
  public void packageLoadBalancingModeWorks() {
    var client = getDbClient();
    try {
      client.init();
      var key = "package-lb-test-" + UUID.randomUUID();
      var values = new HashMap<String, ByteIterator>();
      values.put("field1", new StringByteIterator("value1"));

      var insertStatus = client.insert(TABLE_NAME, key, values);
      assertThat(insertStatus, is(Status.OK));

      var result = new HashMap<String, ByteIterator>();
      var readStatus = client.read(TABLE_NAME, key, null, result);

      assertThat(readStatus, is(Status.OK));
      assertThat(result.get("field1").toString(), is("value1"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      try {
        client.cleanup();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  private static @NotNull DynamoDBClient getDbClient() {
    var props = new Properties();
    props.setProperty("dynamodb.endpoint", endpoint);
    props.setProperty("dynamodb.primaryKey", PRIMARY_KEY);
    props.setProperty("dynamodb.region", "us-east-1");
    props.setProperty("dynamodb.awsAccessKey", "test");
    props.setProperty("dynamodb.awsSecretKey", "test");
    props.setProperty("dynamodb.alternator.loadbalancing", "true");
    props.setProperty("dynamodb.alternator.usePackageLoadBalancer", "true");
    props.setProperty("dynamodb.alternator.restApiEndpoint", restApiEndpoint);

    var client = new DynamoDBClient();
    client.setProperties(props);
    return client;
  }
}
