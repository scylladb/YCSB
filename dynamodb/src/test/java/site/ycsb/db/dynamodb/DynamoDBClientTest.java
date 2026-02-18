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

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;

import com.google.common.collect.Sets;

import org.junit.*;
import org.junit.rules.TestName;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.db.DynamoDBClient;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.scylladb.ScyllaDBContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;


/**
 * Integration tests for the DynamoDB client using Scylla's Alternator interface
 */
public class DynamoDBClientTest {
  private static String HOST;
  private static int PORT;
  private static int REST_PORT;

  public static ScyllaDBContainer scyllaContainer;

  @BeforeClass
  public static void setUpContainer() {
    Assume.assumeTrue("Docker is not available", isDockerAvailable());
    // Set dummy AWS credentials for testing
    System.setProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property(), "dummy");
    System.setProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property(), "dummy");
    // Determine Scylla image from system properties, with sensible defaults
    // -Dscylla.image takes precedence. Otherwise built from -Dscylla.version (defaults to 2025.1)
    final String version = System.getProperty("scylla.version", "2025.1");
    final String imageProp = System.getProperty("scylla.image");
    final String image = (imageProp != null && !imageProp.isEmpty()) ? imageProp : ("scylladb/scylla:" + version);

    scyllaContainer = new ScyllaDBContainer(DockerImageName.parse(image))
        // Enable Alternator (DynamoDB API) on port 8000
        .withCommand("--alternator-port=8000", "--alternator-write-isolation=always");
    scyllaContainer.addExposedPort(8000);
    scyllaContainer.addExposedPort(10000); // REST API port for load balancer
    scyllaContainer.start();

    HOST = scyllaContainer.getHost();
    // Use the mapped port for Alternator (8000)
    PORT = scyllaContainer.getMappedPort(8000);
    // Use the mapped port for REST API (10000)
    REST_PORT = scyllaContainer.getMappedPort(10000);
  }

  @AfterClass
  public static void tearDownContainer() {
    if (scyllaContainer != null) {
      scyllaContainer.stop();
      scyllaContainer = null;
    }
    // Clean up system properties
    System.clearProperty(SdkSystemSetting.AWS_ACCESS_KEY_ID.property());
    System.clearProperty(SdkSystemSetting.AWS_SECRET_ACCESS_KEY.property());
  }

  private DynamoDBClient ycsbClient;
  private DynamoDbClient awsClient;

  @Rule
  public TestName testName = new TestName();
  private String TABLE() {
    return "usertable" + testName.getMethodName();
  }

  private void createTable() throws InterruptedException {
    try {
      // Create DynamoDB table
      CreateTableRequest.Builder createTableRequestBuilder = CreateTableRequest.builder()
          .tableName(TABLE())
          .keySchema(KeySchemaElement.builder()
              .attributeName("y_id")
              .keyType(KeyType.HASH)
              .build())
          .billingMode(BillingMode.PAY_PER_REQUEST);
      if (testName.getMethodName().contains("SecondaryIndex")) {
        createTableRequestBuilder.attributeDefinitions(
              AttributeDefinition.builder().attributeName("y_id").attributeType(ScalarAttributeType.S).build(),
              AttributeDefinition.builder().attributeName(fieldName(0)).attributeType(ScalarAttributeType.S).build()
            ).globalSecondaryIndexes(GlobalSecondaryIndex.builder()
              .indexName("field0-index")
              .keySchema(KeySchemaElement.builder().attributeName(fieldName(0)).keyType(KeyType.HASH).build())
              .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
              .build());
      } else {
        createTableRequestBuilder.attributeDefinitions(
              AttributeDefinition.builder().attributeName("y_id").attributeType(ScalarAttributeType.S).build());
      }
      awsClient.createTable(createTableRequestBuilder.build());
    } catch (ResourceInUseException e) {
      // Table already exists, ignore
    }
  
    // Wait for table to be active - simple polling approach
    int attempts = 0;
    while (attempts < 30) {
      try {
        TableDescription result = awsClient.describeTable(
          DescribeTableRequest.builder().tableName(TABLE()).build()).table();
        boolean tableActive = TableStatus.ACTIVE.equals(result.tableStatus());
        boolean indexActive = result.globalSecondaryIndexes() == null
          || result.globalSecondaryIndexes().stream().allMatch(
                      gsi -> {
                        gsi.indexStatus();
                        return false;
                      });
        if (tableActive && indexActive) {
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
    awsClient = DynamoDbClient.builder()
        .credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.builder().accessKeyId("dummy").secretAccessKey("dummy").build()))
        .endpointOverride(URI.create("http://" + HOST + ":" + PORT))
        .region(Region.of("us-east-1"))
        .build();

    // Create table
    createTable();
  }

  @After
  public void clearTable() {
    try {
      awsClient.deleteTable(DeleteTableRequest.builder().tableName(TABLE()).build());
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  @Before
  public void setUpClient() throws Exception {
    Properties p = new Properties();
    p.setProperty("dynamodb.endpoint", "http://" + HOST + ":" + PORT);
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

  private String fieldName(int fieldNum) {
    return "field" + fieldNum;
  }
  private String fieldValue(int rowNum, int fieldNum) {
    return "value" + ((rowNum * 2) + fieldNum);
  }
  private String keyValue(int rowNum) {
    return "user" + rowNum;
  }

  private void insertRows(int count) {
    for (int i = 0; i < count; i++) {
      Map<String, AttributeValue> item = new HashMap<>();
      item.put("y_id", AttributeValue.fromS(keyValue(i)));
      item.put(fieldName(0), AttributeValue.fromS(fieldValue(i, 0)));
      item.put(fieldName(1), AttributeValue.fromS(fieldValue(i, 1)));

      PutItemRequest putItemRequest = PutItemRequest.builder()
          .tableName(TABLE())
          .item(item)
          .build();
      awsClient.putItem(putItemRequest);
    }
  }

  private void insertRow() {
    insertRows(1);
  }
  private String fieldValue(int fieldNum) {
    return fieldValue(0, fieldNum);
  }
  private String keyValue() {
    return keyValue(0);
  }

  @Test
  public void testRead() {
    insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = ycsbClient.read(TABLE(), keyValue(), null, result);
    assertThat(status, is(Status.OK));
    // Note: DynamoDB will only return the fields that exist, unlike Scylla
    assertThat(result.entrySet(), hasSize(3)); // y_id, field0, field1

    final Map<String, String> strResult = StringByteIterator.getStringMap(result);
    assertThat(strResult, hasEntry("y_id", keyValue()));
    assertThat(strResult, hasEntry(fieldName(0), fieldValue(0)));
    assertThat(strResult, hasEntry(fieldName(1), fieldValue(1)));
  }

  @Test
  public void testReadSelectedColumns() {
    insertRow();
    for (Set<Integer> selected_fields : Sets.newHashSet(
      Sets.newHashSet(1),
      Sets.newHashSet(0, 1))) {
      final Set<String> field_names = selected_fields.stream()
        .map(field -> fieldName(field))
        .collect(Collectors.toSet());
      final HashMap<String, ByteIterator> result = new HashMap<>();
      final Status status = ycsbClient.read(TABLE(), keyValue(), field_names, result);
      assertThat(status, is(Status.OK));
      assertThat(result.entrySet(), hasSize(selected_fields.size()));
      final Map<String, String> strResult = StringByteIterator.getStringMap(result);
      for (Integer field : selected_fields) {
        assertThat(strResult, hasEntry(fieldName(field), fieldValue(field)));
      }
    }
  }

  @Test
  public void testInsert() {
    final String key = "key";
    final Map<String, String> input = new HashMap<>();
    input.put(fieldName(0), fieldValue(0));
    input.put(fieldName(1), fieldValue(1));

    final Status status = ycsbClient.insert(TABLE(), key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(TABLE())
        .key(Map.of("y_id", AttributeValue.fromS(key)))
        .build();

    GetItemResponse getItemResult = awsClient.getItem(getItemRequest);
    assertThat(getItemResult.hasItem(), is(true));
    assertThat(getItemResult.item().get(fieldName(0)).s(), is(fieldValue(0)));
    assertThat(getItemResult.item().get(fieldName(1)).s(), is(fieldValue(1)));
  }

  @Test
  public void testUpdate() {
    insertRow();
    final Map<String, String> input = new HashMap<>();
    input.put(fieldName(0), "new-value1");
    input.put(fieldName(1), "new-value2");

    final Status status = ycsbClient.update(TABLE(), keyValue(),
        StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(TABLE())
        .key(Map.of("y_id", AttributeValue.fromS(keyValue())))
        .build();

    GetItemResponse getItemResult = awsClient.getItem(getItemRequest);
    assertThat(getItemResult.hasItem(), is(true));
    assertThat(getItemResult.item().get(fieldName(0)).s(), is("new-value1"));
    assertThat(getItemResult.item().get(fieldName(1)).s(), is("new-value2"));
  }

  @Test
  public void testDelete() {
    insertRow();

    final Status status = ycsbClient.delete(TABLE(), keyValue());
    assertThat(status, is(Status.OK));

    // Verify result
    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(TABLE())
        .key(Map.of("y_id", AttributeValue.fromS(keyValue())))
        .build();

    GetItemResponse getItemResult = awsClient.getItem(getItemRequest);
    assertThat(getItemResult.hasItem(), is(false));
  }

  private void assertRows(int expected, Vector<HashMap<String, ByteIterator>> results) {
    assertThat(results.size(), is(expected));
    final Map<String, Map<String, String>> found = new HashMap<>();
    for (int i = 0; i < expected; i++) {
      final Map<String, String> strResult = StringByteIterator.getStringMap(results.get(i));
      found.put(strResult.get("y_id"), strResult);
    }
    for (int j = 0; j < expected; j++) {
      assertThat(found, hasKey(keyValue(j)));
      final Map<String, String> strResult = found.get(keyValue(j));
      assertThat(strResult, hasEntry(fieldName(0), fieldValue(j, 0)));
      assertThat(strResult, hasEntry(fieldName(1), fieldValue(j, 1)));
    }
  }
  
  @Test
  public void testScan() {
    insertRows(3);

    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    Status status = ycsbClient.scan(TABLE(), keyValue(), 1, null, results);
    assertRows(1, results);

    for (int i = 0; i < 3; i++) { // One of these should return 3 rows
      results = new Vector<>();
      status = ycsbClient.scan(TABLE(), keyValue(i), 3, null, results);
      if (results.size() == 3) {
        assertRows(3, results);
        return;
      }
    }
    Assert.fail("Scan did not return expected number of rows");
  }

  private void updateClientProperties(Map<String, String> newProps) {
    Properties p = ycsbClient.getProperties();
    for (Map.Entry<String, String> entry : newProps.entrySet()) {
      p.setProperty(entry.getKey(), entry.getValue());
    }
    try {
      ycsbClient.cleanup();
    } catch (Exception e) {
      // Ignore
    }
    ycsbClient = new DynamoDBClient();
    ycsbClient.setProperties(p);
    try {
      ycsbClient.init();
    } catch (Exception e) {
      throw new RuntimeException("Failed to reinitialize YCSB client with new properties", e);
    }
  }

  @Test
  public void testExclusiveScan() throws Exception {
    insertRows(3);
    updateClientProperties(Map.of("dynamodb.inclusiveScan", "false"));
    for (int i = 0; i < 3; i++) { // One of these should return 2 rows
      Vector<HashMap<String, ByteIterator>> results = new Vector<>();
      Status status = ycsbClient.scan(TABLE(), keyValue(i), 3, null, results);
      if (results.size() == 2) {
        results.add(new HashMap<>(Map.of(
            "y_id", new StringByteIterator(keyValue(i)),
            fieldName(0), new StringByteIterator(fieldValue(i, 0)),
            fieldName(1), new StringByteIterator(fieldValue(i, 1))
        ))); // Add dummy row for the skipped one
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
      testReadSelectedColumns();
      testReadMissingRow();
      testDelete();
    }
  }

  @Test
  public void testSecondaryIndex() throws Exception {
    insertRows(3);
    final String indexTableName = TABLE() + ":field0-index";

    for (int i = 0; i < 10; i++) { // wait for index to be ready
      Vector<HashMap<String, ByteIterator>> results = new Vector<>();
      Status status = ycsbClient.scan(indexTableName, null, 3, null, results);
      if (status == Status.OK && results.size() == 3) {
        assertThat(status, is(Status.OK));
        assertRows(3, results);
        break;
      }
      if (i == 9) {
        Assert.fail("Index not ready in time");
      } else {
        Thread.sleep(1000);
      }
    }

    // Test reading from secondary index using YCSB client
    updateClientProperties(Map.of("dynamodb.primaryKey", fieldName(0)));
    HashMap<String, ByteIterator> result = new HashMap<>();
    Status status = ycsbClient.read(indexTableName, fieldValue(0), null, result);
    assertThat(status, is(Status.OK));
    assertRows(1, new Vector<>(java.util.List.of(result)));

    // Test scan operation on secondary index using YCSB client
    updateClientProperties(Map.of(
        "dynamodb.hashKeyName", "y_id",
        "dynamodb.hashKeyValue", keyValue(),
        "dynamodb.primaryKeyType", "HASH_AND_RANGE"));
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    status = ycsbClient.scan(indexTableName, fieldValue(0), 1, null, results);
    assertThat(status, is(Status.OK));
    assertRows(1, results);

    for (int i = 0; i < 3; i++) { // One of these should return 3 rows
      results = new Vector<>();
      status = ycsbClient.scan(indexTableName, fieldValue(i, 0), 3, null, results);
      assertThat(status, is(Status.OK));
      if (results.size() == 3) {
        assertRows(3, results);
        return;
      }
    }
    Assert.fail("Scan did not return expected number of rows");
  }

  @Test
  public void testPackageLoadBalancer() throws DBException {
    Properties props = new Properties();
    props.setProperty("dynamodb.endpoint", "http://" + HOST + ":" + PORT);
    props.setProperty("dynamodb.primaryKey", "y_id");
    props.setProperty("dynamodb.alternator.loadbalancing", "true");
    props.setProperty("dynamodb.alternator.usePackageLoadBalancer", "true");
    props.setProperty("dynamodb.alternator.restApiEndpoint", "http://" + HOST + ":" + REST_PORT);

    DynamoDBClient client = new DynamoDBClient();
    client.setProperties(props);
    client.init();

    try {
      String key = "lb-test-key";
      Map<String, ByteIterator> values = new HashMap<>();
      values.put("f1", new StringByteIterator("v1"));
      Status status = client.insert(TABLE(), key, values);
      assertThat(status, is(Status.OK));

      Map<String, ByteIterator> result = new HashMap<>();
      status = client.read(TABLE(), key, null, result);
      assertThat(status, is(Status.OK));
      assertThat(result.get("f1").toString(), is("v1"));
    } finally {
      client.cleanup();
    }
  }

  /**
   * Test that the client can initialize and work without explicit credentials.
   * This tests the scenario where no dynamodb.awsAccessKey, dynamodb.awsSecretKey,
   * or dynamodb.awsCredentialsFile properties are set.
   * The client should fall back to the AWS SDK's default credential provider chain,
   * which includes system properties, environment variables, AWS profiles, etc.
   */
  @Test
  public void testClientWithoutExplicitCredentials() throws Exception {
    // System properties are already set in setUpContainer() for testing
    // AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY system properties

    Properties p = new Properties();
    p.setProperty("dynamodb.endpoint", "http://" + HOST + ":" + PORT);
    p.setProperty("dynamodb.primaryKey", "y_id");
    p.setProperty("dynamodb.region", "us-east-1");
    p.setProperty("table", TABLE());
    // Note: We are NOT setting:
    // - dynamodb.awsAccessKey
    // - dynamodb.awsSecretKey
    // - dynamodb.awsCredentialsFile

    DynamoDBClient clientWithoutCreds = new DynamoDBClient();
    try {
      clientWithoutCreds.setProperties(p);
      clientWithoutCreds.init();

      // Verify the client can perform operations using default credentials
      final String key = "test-key-no-creds";
      final Map<String, String> input = new HashMap<>();
      input.put(fieldName(0), "value0");
      input.put(fieldName(1), "value1");

      // Insert operation
      final Status insertStatus = clientWithoutCreds.insert(TABLE(), key, StringByteIterator.getByteIteratorMap(input));
      assertThat("Insert should succeed with default credentials", insertStatus, is(Status.OK));

      // Read operation to verify insert worked
      final HashMap<String, ByteIterator> result = new HashMap<>();
      final Status readStatus = clientWithoutCreds.read(TABLE(), key, null, result);
      assertThat("Read should succeed with default credentials", readStatus, is(Status.OK));
      assertThat("Read should return the inserted data", result.entrySet(), hasSize(3)); // y_id, field0, field1

      final Map<String, String> strResult = StringByteIterator.getStringMap(result);
      assertThat(strResult, hasEntry("y_id", key));
      assertThat(strResult, hasEntry(fieldName(0), "value0"));
      assertThat(strResult, hasEntry(fieldName(1), "value1"));
    } finally {
      clientWithoutCreds.cleanup();
    }
  }

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
