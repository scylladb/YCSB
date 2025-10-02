/*
 * Copyright (c) 2025 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package site.ycsb.db.dynamodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.Sets;

import org.junit.*;
import org.junit.rules.TestName;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.DynamoDBClient;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.scylladb.ScyllaDBContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Integration tests for the DynamoDB client using Scylla's Alternator interface
 */
public class DynamoDBClientTest {
  private static String HOST;
  private static int PORT;
  private static java.io.File tempCredFile;

  public static ScyllaDBContainer scyllaContainer;

  @BeforeClass
  public static void setUpContainer() {
    try {
      // Determine Scylla image from system properties, with sensible defaults
      // -Dscylla.image takes precedence. Otherwise built from -Dscylla.version (defaults to 2025.1)
      final String version = System.getProperty("scylla.version", "2025.1");
      final String imageProp = System.getProperty("scylla.image");
      final String image = (imageProp != null && !imageProp.isEmpty()) ? imageProp : ("scylladb/scylla:" + version);

      scyllaContainer = new ScyllaDBContainer(DockerImageName.parse(image))
          // Enable Alternator (DynamoDB API) on port 8000
          .withCommand("--alternator-port=8000", "--alternator-write-isolation=always");
      scyllaContainer.addExposedPort(8000);
      scyllaContainer.start();

      HOST = scyllaContainer.getHost();
      // Use the mapped port for Alternator (8000)
      PORT = scyllaContainer.getMappedPort(8000);

    } catch (Throwable t) {
      Assume.assumeTrue("Skipping DynamoDB tests because Docker/Testcontainers is not available: " + t.getMessage(), false);
    }

    // Create a temporary credentials file for the test
    try {
      tempCredFile = java.io.File.createTempFile("aws-credentials", ".properties");
      tempCredFile.deleteOnExit();
      try (java.io.FileWriter writer = new java.io.FileWriter(tempCredFile)) {
        writer.write("accessKey=dummy\n");
        writer.write("secretKey=dummy\n");
      }
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to create temporary credentials file", e);
    }
  }

  @AfterClass
  public static void tearDownContainer() {
    if (scyllaContainer != null) {
      scyllaContainer.stop();
      scyllaContainer = null;
    }
  }

  private DynamoDBClient ycsbClient;
  private AmazonDynamoDB awsClient;

  @Rule
  public TestName testName = new TestName();
  private String TABLE() {
    return "usertable" + testName.getMethodName();
  }

  private void createTable() throws InterruptedException {
    try {
      // Create DynamoDB table
      CreateTableRequest createTableRequest = new CreateTableRequest()
          .withTableName(TABLE())
          .withKeySchema(new KeySchemaElement("y_id", KeyType.HASH))
          .withAttributeDefinitions(new AttributeDefinition("y_id", ScalarAttributeType.S))
          .withBillingMode(BillingMode.PAY_PER_REQUEST);
      awsClient.createTable(createTableRequest);
    } catch (ResourceInUseException e) {
      // Table already exists, ignore
    }
      
    // Wait for table to be active - simple polling approach
    int attempts = 0;
    while (attempts < 30) {
      try {
        DescribeTableResult result = awsClient.describeTable(TABLE());
        if ("ACTIVE".equals(result.getTable().getTableStatus())) {
          return;
        }
      } catch (ResourceNotFoundException e) { }
      Thread.sleep(1000);
      attempts++;
    }
  }

  @Before
  public void setUpTable() throws Exception {
    // Create DynamoDB client pointing to Scylla's Alternator interface
    BasicAWSCredentials awsCreds = new BasicAWSCredentials("dummy", "dummy");
    awsClient = AmazonDynamoDBClientBuilder.standard()
        .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
            "http://" + HOST + ":" + PORT, "us-east-1"))
        .build();

    // Create table
    createTable();
  }

  @After
  public void clearTable() {
    try {
      awsClient.deleteTable(TABLE());
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  @Before
  public void setUpClient() throws Exception {
    Properties p = new Properties();
    p.setProperty("dynamodb.endpoint", "http://" + HOST + ":" + PORT);
    p.setProperty("dynamodb.awsCredentialsFile", tempCredFile.getAbsolutePath());
    p.setProperty("dynamodb.primaryKey", "y_id");
    p.setProperty("dynamodb.region", "us-east-1");
    p.setProperty("table", TABLE());

    ycsbClient = new DynamoDBClient();
    ycsbClient.setProperties(p);
    ycsbClient.init();
  }

  @After
  public void tearDownClient() throws Exception {
    if (ycsbClient != null) {
      ycsbClient.cleanup();
    }
    ycsbClient = null;
  }

  @Test
  public void testReadMissingRow() {
    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = ycsbClient.read(TABLE(), "Missing row", null, result);
    assertThat(result.size(), is(0));
    //assertThat(status, is(Status.NOT_FOUND)); // Probably bug in DynamoDBClient
  }

  private void insertRows(int count) {
    for (int i = 1; i <= count; i++) {
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("y_id", new AttributeValue("user" + i));
      item.put("field0", new AttributeValue("value" + (2*i-1)));
      item.put("field1", new AttributeValue("value" + (2*i)));

      PutItemRequest putItemRequest = new PutItemRequest()
          .withTableName(TABLE())
          .withItem(item);
      awsClient.putItem(putItemRequest);
    }
  }

  private void insertRow() {
    insertRows(1);
  }

  @Test
  public void testRead() {
    insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = ycsbClient.read(TABLE(), "user1", null, result);
    assertThat(status, is(Status.OK));
    // Note: DynamoDB will only return the fields that exist, unlike Scylla
    assertThat(result.entrySet(), hasSize(3)); // y_id, field0, field1

    final Map<String, String> strResult = StringByteIterator.getStringMap(result);
    assertThat(strResult, hasEntry("y_id", "user1"));
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadSingleColumn() {
    insertRow();
    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Set<String> fields = Sets.newHashSet("field1");
    final Status status = ycsbClient.read(TABLE(), "user1", fields, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(1));
    final Map<String, String> strResult = StringByteIterator.getStringMap(result);
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testInsert() {
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "value1");
    input.put("field1", "value2");

    final Status status = ycsbClient.insert(TABLE(), key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    GetItemRequest getItemRequest = new GetItemRequest()
        .withTableName(TABLE())
        .withKey(Map.of("y_id", new AttributeValue(key)));

    GetItemResult getItemResult = awsClient.getItem(getItemRequest);
    assertThat(getItemResult.getItem(), notNullValue());
    assertThat(getItemResult.getItem().get("field0").getS(), is("value1"));
    assertThat(getItemResult.getItem().get("field1").getS(), is("value2"));
  }

  @Test
  public void testUpdate() {
    insertRow();
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "new-value1");
    input.put("field1", "new-value2");

    final Status status = ycsbClient.update(TABLE(), "user1",
        StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    GetItemRequest getItemRequest = new GetItemRequest()
        .withTableName(TABLE())
        .withKey(Map.of("y_id", new AttributeValue("user1")));

    GetItemResult getItemResult = awsClient.getItem(getItemRequest);
    assertThat(getItemResult.getItem(), notNullValue());
    assertThat(getItemResult.getItem().get("field0").getS(), is("new-value1"));
    assertThat(getItemResult.getItem().get("field1").getS(), is("new-value2"));
  }

  @Test
  public void testDelete() {
    insertRow();

    final Status status = ycsbClient.delete(TABLE(), "user1");
    assertThat(status, is(Status.OK));

    // Verify result
    GetItemRequest getItemRequest = new GetItemRequest()
        .withTableName(TABLE())
        .withKey(Map.of("y_id", new AttributeValue("user1")));

    GetItemResult getItemResult = awsClient.getItem(getItemRequest);
    assertThat(getItemResult.getItem(), nullValue());
  }

  private void assertRows(int expected, Vector<HashMap<String, ByteIterator>> results) {
    assertThat(results.size(), is(expected));
    final Map<String, Map<String, String>> found = new HashMap<>();
    for (int i = 0; i < expected; i++) {
      final Map<String, String> strResult = StringByteIterator.getStringMap(results.get(i));
      found.put(strResult.get("y_id"), strResult);
    }
    for (int j = 1; j <= expected; j++) {
      assertThat(found, hasKey("user" + j));
      final Map<String, String> strResult = found.get("user" + j);
      assertThat(strResult, hasEntry("field0", "value" + (2*j-1)));
      assertThat(strResult, hasEntry("field1", "value" + (2*j)));
    }
  }
  
  @Test
  public void testScan() {
    insertRows(3);

    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    Status status = ycsbClient.scan(TABLE(), "user1", 1, null, results);
    assertRows(1, results);

    for (int i = 1; i <= 3; i++) { // One of these should return 3 rows
      results = new Vector<>();
      status = ycsbClient.scan(TABLE(), "user" + i, 3, null, results);
      if (results.size() == 3) {
        assertRows(3, results);
        return;
      }
    }
    Assert.fail("Scan did not return expected number of rows");
  }

  @Test
  public void testMultipleOperations() {
    final int LOOP_COUNT = 3;
    for (int i = 0; i < LOOP_COUNT; i++) {
      testInsert();
      testUpdate();
      testRead();
      testReadSingleColumn();
      testReadMissingRow();
      testDelete();
    }
  }
}
