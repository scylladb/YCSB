/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2026 YCSB Contributors. All Rights Reserved.
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

package site.ycsb.db;

import com.scylladb.alternator.AlternatorConfig;
import com.scylladb.alternator.AlternatorDynamoDbAsyncClient;
import com.scylladb.alternator.routing.ClusterScope;
import com.scylladb.alternator.routing.DatacenterScope;
import com.scylladb.alternator.routing.RackScope;
import com.scylladb.alternator.routing.RoutingScope;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.TlsTrustManagersProvider;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.endpoints.DynamoDbEndpointProvider;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.net.URI;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * DynamoDB client for YCSB - Java 21 modernized.
 *
 * <p>Supports both AWS DynamoDB and ScyllaDB Alternator with client-side load balancing.
 */
public final class DynamoDBClient extends DB {

  private static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";
  private static final long MILLIS_TO_SECONDS = 1000L;
  private static final Lock CLIENT_LOCK = new ReentrantLock();
  private static final java.util.concurrent.atomic.AtomicInteger CLIENT_REF_COUNT = new java.util.concurrent.atomic.AtomicInteger();

  private static volatile DynamoDbAsyncClient sharedClient;
  private static volatile ExecutorService sharedCompletionExecutor;
  private static volatile DynamoDbEndpointProvider sharedEndpointProvider;

  private DynamoDbAsyncClient dynamoDbClient;
  private DynamoDBConfig config;

  /**
   * Configuration record for DynamoDB client settings.
   */
  private record DynamoDBConfig(String primaryKeyName, PrimaryKeyType primaryKeyType, String hashKeyName,
                                String hashKeyValue, String ttlKeyName, long ttlDuration, boolean consistentRead,
                                boolean inclusiveScan, boolean useLegacyAPI) {
    static DynamoDBConfig from(java.util.Properties props) throws DBException {
      var primaryKey = props.getProperty("dynamodb.primaryKey");
      if (primaryKey == null || primaryKey.isEmpty()) {
        throw new DBException("Missing primary key attribute name, cannot continue");
      }

      var primaryKeyType = parsePrimaryKeyType(props.getProperty("dynamodb.primaryKeyType"));
      var hashKeyName = "";
      var hashKeyValue = DEFAULT_HASH_KEY_VALUE;

      if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
        hashKeyName = props.getProperty("dynamodb.hashKeyName", "");
        if (hashKeyName.isEmpty()) {
          throw new DBException("Must specify hash key name when primary key type is HASH_AND_RANGE.");
        }
        hashKeyValue = props.getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
      }

      var ttlKeyName = props.getProperty("dynamodb.ttlKey");
      var ttlDurationStr = props.getProperty("dynamodb.ttlDuration");
      var ttlDuration = 0L;
      if (ttlKeyName != null && ttlDurationStr != null) {
        ttlDuration = Long.parseLong(ttlDurationStr);
      } else {
        ttlKeyName = null;
      }

      return new DynamoDBConfig(primaryKey, primaryKeyType, hashKeyName, hashKeyValue, ttlKeyName, ttlDuration, Boolean.parseBoolean(props.getProperty("dynamodb.consistentReads", "false")), !"false".equalsIgnoreCase(props.getProperty("dynamodb.inclusiveScan")), Boolean.parseBoolean(props.getProperty("dynamodb.useLegacyAPI", "false")));
    }

    private static PrimaryKeyType parsePrimaryKeyType(String value) throws DBException {
      if (value == null || value.isEmpty()) {
        return PrimaryKeyType.HASH;
      }
      try {
        return PrimaryKeyType.valueOf(value.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid primary key mode: " + value + ". Expected HASH or HASH_AND_RANGE.");
      }
    }
  }

  private enum PrimaryKeyType {HASH, HASH_AND_RANGE}

  private record TableIndex(String table, Optional<String> index) {
    static TableIndex parse(String input) {
      var parts = input.split(":", 2);
      return new TableIndex(parts[0], parts.length > 1 ? Optional.of(parts[1]) : Optional.empty());
    }
  }

  private record InclusiveScanResult(Status status, int count) {
  }

  @Override
  public void init() throws DBException {
    var props = getProperties();

    if ("true".equalsIgnoreCase(props.getProperty("dynamodb.debug"))) {
      LOGGER.setLevel(Level.DEBUG);
    }

    this.config = DynamoDBConfig.from(props);

    CLIENT_LOCK.lock();
    try {
      if (sharedClient == null) {
        initializeSharedClient(props);
      }
      this.dynamoDbClient = sharedClient;
      CLIENT_REF_COUNT.incrementAndGet();
    } finally {
      CLIENT_LOCK.unlock();
    }

    logDebug(() -> config.ttlKeyName() != null ? "TTL configured: key=" + config.ttlKeyName() + ", duration=" + config.ttlDuration() : "No TTL configured");
  }

  private void initializeSharedClient(java.util.Properties props) {
    var builder = AlternatorDynamoDbAsyncClient.builder();

    var region = Optional.ofNullable(props.getProperty("dynamodb.region")).map(Region::of).orElse(Region.US_EAST_1);
    builder.region(region);

    var endpoint = props.getProperty("dynamodb.endpoint");
    LOGGER.info("Using DynamoDB endpoint: " + endpoint);
    var useLoadBalancing = Boolean.parseBoolean(props.getProperty("dynamodb.alternator.loadbalancing", "true"));
    LOGGER.info("Alternator load balancing enabled: " + useLoadBalancing);

    if (useLoadBalancing && endpoint != null) {
      builder.withAlternatorConfig(createEndpointProvider(props, endpoint));
      LOGGER.info("Alternator load balancing enabled with seed: " + endpoint);
    } else if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }

    var accessKey = props.getProperty("dynamodb.awsAccessKey", "test");
    var secretKey = props.getProperty("dynamodb.awsSecretKey", "test");
    var credentialsFile = props.getProperty("dynamodb.awsCredentialsFile");

    if (credentialsFile != null && !credentialsFile.isEmpty()) {
      var credentials = loadCredentialsFromFile(credentialsFile);
      builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
    } else if (accessKey != null && secretKey != null) {
      builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    }

    var threadCount = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    var httpClientBuilder = NettyNioAsyncHttpClient.builder().maxConcurrency(threadCount);

    // Configure SSL/TLS trust all certificates if requested
    var trustAllCerts = Boolean.parseBoolean(props.getProperty("dynamodb.alternator.trustAllCertificates", "false"));
    if (trustAllCerts) {
      LOGGER.warn("Trust all certificates is enabled. This should only be used for testing with self-signed certificates. Never enable in production!");
      httpClientBuilder.tlsTrustManagersProvider(createTrustAllTrustManagersProvider());
    }

    builder.httpClientBuilder(httpClientBuilder);

    // Optional: attach custom execution interceptors (useful for observability/tests)
    var interceptors = props.getProperty("dynamodb.executionInterceptors");
    if (interceptors != null && !interceptors.isEmpty()) {
      var cfg = software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.builder();
      for (var name : interceptors.split(",")) {
        var cn = name.trim();
        if (cn.isEmpty()) {
          continue;
        }
        try {
          var clazz = Class.forName(cn);
          var interceptor = (software.amazon.awssdk.core.interceptor.ExecutionInterceptor) clazz.getDeclaredConstructor().newInstance();
          cfg.addExecutionInterceptor(interceptor);
        } catch (Throwable t) {
          LOGGER.warn("Failed to add execution interceptor: " + cn, t);
        }
      }
      builder.overrideConfiguration(cfg.build());
    }

    var useVirtualThreads = Boolean.parseBoolean(props.getProperty("dynamodb.virtualThreads", "false"));
    if (useVirtualThreads) {
      sharedCompletionExecutor = Executors.newVirtualThreadPerTaskExecutor();
      builder.asyncConfiguration(ClientAsyncConfiguration.builder().advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, sharedCompletionExecutor).build());
    }

    sharedClient = builder.build();
  }

  private AlternatorConfig createEndpointProvider(java.util.Properties props, String endpoint) {
    String datacenter = props.getProperty("dynamodb.alternator.datacenter");
    String rack = props.getProperty("dynamodb.alternator.rack");
    RoutingScope scope = RackScope.of(datacenter, rack, DatacenterScope.of(datacenter, ClusterScope.create()));

    return AlternatorConfig.builder().withSeedNode(URI.create(endpoint)).withRoutingScope(scope).withOptimizeHeaders(true).build();
  }

  /**
   * Loads AWS credentials from a file.
   * The file should be in Java Properties format with:
   * accessKey = your_access_key_id
   * secretKey = your_secret_access_key
   *
   * @param filePath the path to the credentials file
   * @return AWS credentials loaded from the file
   * @throws RuntimeException if the file cannot be read or is malformed
   */
  private AwsCredentials loadCredentialsFromFile(String filePath) {
    try {
      var props = new Properties();
      try (var input = new FileInputStream(filePath)) {
        props.load(input);
      }

      var accessKey = props.getProperty("accessKey");
      var secretKey = props.getProperty("secretKey");

      if (accessKey == null || accessKey.trim().isEmpty()) {
        throw new IllegalArgumentException("Credentials file must contain 'accessKey' property");
      }

      if (secretKey == null || secretKey.trim().isEmpty()) {
        throw new IllegalArgumentException("Credentials file must contain 'secretKey' property");
      }

      LOGGER.info("Loaded AWS credentials from file: " + filePath);
      return AwsBasicCredentials.create(accessKey.trim(), secretKey.trim());
    } catch (IOException e) {
      throw new RuntimeException("Failed to read AWS credentials from file: " + filePath, e);
    }
  }

  /**
   * Creates a TrustManagersProvider that trusts all certificates.
   * This should only be used for testing with self-signed certificates.
   * Never use this in production environments.
   *
   * @return TrustManagersProvider that accepts all certificates
   */
  private TlsTrustManagersProvider createTrustAllTrustManagersProvider() {
    return () -> new TrustManager[]{new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] chain, String authType) {
        // Accept all client certificates
      }

      @Override
      public void checkServerTrusted(X509Certificate[] chain, String authType) {
        // Accept all server certificates
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }
    }};
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    var tableIndex = TableIndex.parse(table);

    return tableIndex.index().map(idx -> readWithIndex(tableIndex.table(), idx, key, fields, result)).orElseGet(() -> {
      logDebug(() -> "readkey: " + key + " from table: " + tableIndex.table());
      return getItem(tableIndex.table(), createPrimaryKey(key), fields, result, false);
    });
  }

  private Status readWithIndex(String table, String index, String key, Set<String> fields, Map<String, ByteIterator> result) {
    logDebug(() -> "readkey: " + key + " from table: " + table + " with index: " + index);

    var tempResult = new Vector<HashMap<String, ByteIterator>>();
    var status = query(table, index, createPrimaryKey(key), 1, fields, tempResult);

    if (status == Status.OK && !tempResult.isEmpty()) {
      result.putAll(tempResult.getFirst());
    }
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var tableIndex = TableIndex.parse(table);
    logDebug(() -> "scan " + recordcount + " records from key: " + startkey + " on table: " + tableIndex.table());

    var startKey = (startkey == null || startkey.isEmpty()) ? null : createPrimaryKey(startkey);
    var count = 0;

    if (startKey != null && config.inclusiveScan()) {
      var inclusiveResult = handleInclusiveScan(tableIndex, startKey, recordcount, fields, result);
      if (inclusiveResult.status() != Status.OK) {
        return inclusiveResult.status();
      }
      count = inclusiveResult.count();
    }

    return performScan(tableIndex, startKey, recordcount, count, fields, result);
  }

  private InclusiveScanResult handleInclusiveScan(TableIndex tableIndex, Map<String, AttributeValue> startKey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return tableIndex.index().map(idx -> {
      var tempResult = new Vector<HashMap<String, ByteIterator>>();
      var status = query(tableIndex.table(), idx, startKey, recordcount, fields, tempResult);
      result.addAll(tempResult);
      return new InclusiveScanResult(status, tempResult.size());
    }).orElseGet(() -> {
      var tempResult = new HashMap<String, ByteIterator>();
      var status = getItem(tableIndex.table(), startKey, fields, tempResult, true);
      if (!tempResult.isEmpty()) {
        result.add(new HashMap<>(tempResult));
        return new InclusiveScanResult(status, 1);
      }
      return new InclusiveScanResult(status, 0);
    });
  }

  private Status performScan(TableIndex tableIndex, Map<String, AttributeValue> startKey, int recordcount, int initialCount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var scanBuilder = ScanRequest.builder().tableName(tableIndex.table());
    tableIndex.index().ifPresent(scanBuilder::indexName);
    configureProjection(scanBuilder, fields);

    var count = initialCount;
    var currentStartKey = startKey;

    while (count < recordcount) {
      if (currentStartKey != null) {
        scanBuilder.exclusiveStartKey(currentStartKey);
      }
      scanBuilder.limit(recordcount - count);

      try {
        var response = dynamoDbClient.scan(scanBuilder.build()).join();
        count += response.count();
        response.items().stream().map(this::extractResult).forEach(result::add);

        if (!response.hasLastEvaluatedKey()) {
          break;
        }
        currentStartKey = response.lastEvaluatedKey();
      } catch (CompletionException e) {
        return handleAsyncException(e);
      }
    }
    return Status.OK;
  }

  private void configureProjection(ScanRequest.Builder builder, Set<String> fields) {
    if (config.useLegacyAPI()) {
      builder.attributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      var aliases = aliasFields(fields);
      builder.expressionAttributeNames(aliases).projectionExpression(String.join(",", aliases.keySet()));
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    var tableName = TableIndex.parse(table).table();
    logDebug(() -> "updatekey: " + key + " from table: " + tableName);

    var updateBuilder = UpdateItemRequest.builder().key(createPrimaryKey(key)).tableName(tableName);

    if (config.useLegacyAPI()) {
      updateBuilder.attributeUpdates(createLegacyUpdates(values));
    } else {
      configureExpressionUpdate(updateBuilder, values);
    }

    try {
      dynamoDbClient.updateItem(updateBuilder.build()).join();
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private Map<String, AttributeValueUpdate> createLegacyUpdates(Map<String, ByteIterator> values) {
    var updates = new HashMap<String, AttributeValueUpdate>(values.size() + 1);
    values.forEach((k, v) -> updates.put(k, AttributeValueUpdate.builder().action(AttributeAction.PUT).value(AttributeValue.fromS(v.toString())).build()));

    if (config.ttlKeyName() != null) {
      updates.put(config.ttlKeyName(), AttributeValueUpdate.builder().action(AttributeAction.PUT).value(AttributeValue.fromN(String.valueOf(currentTtl()))).build());
    }
    return updates;
  }

  private void configureExpressionUpdate(UpdateItemRequest.Builder builder, Map<String, ByteIterator> values) {
    var attrNames = new HashMap<String, String>();
    var attrValues = new HashMap<String, AttributeValue>();
    var expression = new StringBuilder("SET ");
    var first = true;

    for (var entry : values.entrySet()) {
      if (!first) {
        expression.append(",");
      }
      var nameAlias = addAlias("#", entry.getKey(), attrNames);
      var valueAlias = addAlias(":", AttributeValue.fromS(entry.getValue().toString()), attrValues);
      expression.append(nameAlias).append("=").append(valueAlias);
      first = false;
    }

    if (config.ttlKeyName() != null) {
      if (!first) {
        expression.append(",");
      }
      var nameAlias = addAlias("#", config.ttlKeyName(), attrNames);
      var valueAlias = addAlias(":", AttributeValue.fromN(String.valueOf(currentTtl())), attrValues);
      expression.append(nameAlias).append("=").append(valueAlias);
    }

    builder.expressionAttributeNames(attrNames).expressionAttributeValues(attrValues).updateExpression(expression.toString());
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    var tableName = TableIndex.parse(table).table();
    logDebug(() -> "insertkey: " + config.primaryKeyName() + "-" + key + " from table: " + tableName);

    var attributes = createInsertAttributes(key, values);
    try {
      dynamoDbClient.putItem(PutItemRequest.builder().item(attributes).tableName(tableName).build()).join();
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private Map<String, AttributeValue> createInsertAttributes(String key, Map<String, ByteIterator> values) {
    var attributes = values.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> AttributeValue.fromS(e.getValue().toString()), (a, b) -> a, HashMap::new));

    attributes.put(config.primaryKeyName(), AttributeValue.fromS(key));

    if (config.primaryKeyType() == PrimaryKeyType.HASH_AND_RANGE) {
      attributes.put(config.hashKeyName(), AttributeValue.fromS(config.hashKeyValue()));
    }

    if (config.ttlKeyName() != null) {
      attributes.put(config.ttlKeyName(), AttributeValue.fromN(String.valueOf(currentTtl())));
    }
    return attributes;
  }

  @Override
  public Status delete(String table, String key) {
    var tableName = TableIndex.parse(table).table();
    logDebug(() -> "deletekey: " + key + " from table: " + tableName);

    try {
      dynamoDbClient.deleteItem(DeleteItemRequest.builder().key(createPrimaryKey(key)).tableName(tableName).build()).join();
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    CLIENT_LOCK.lock();
    try {
      if (CLIENT_REF_COUNT.decrementAndGet() > 0) {
        return;
      }

      if (sharedCompletionExecutor != null) {
        sharedCompletionExecutor.shutdown();
        sharedCompletionExecutor = null;
      }
      if (sharedEndpointProvider != null) {
        if (sharedEndpointProvider instanceof AutoCloseable) {
          try {
            ((AutoCloseable) sharedEndpointProvider).close();
          } catch (Exception e) {
            LOGGER.warn("Error closing endpoint provider", e);
          }
        }
        sharedEndpointProvider = null;
      }
      if (sharedClient != null) {
        sharedClient.close();
        sharedClient = null;
      }
    } finally {
      CLIENT_LOCK.unlock();
    }
  }

  private Status getItem(String table, Map<String, AttributeValue> key, Set<String> fields, Map<String, ByteIterator> result, boolean inScan) {
    var builder = GetItemRequest.builder().key(key).tableName(table);

    if (config.useLegacyAPI()) {
      builder.attributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      var aliases = aliasFields(fields);
      builder.expressionAttributeNames(aliases).projectionExpression(String.join(",", aliases.keySet()));
    }

    if (!inScan) {
      builder.consistentRead(config.consistentRead());
    }

    try {
      var response = dynamoDbClient.getItem(builder.build()).join();
      if (response.hasItem()) {
        result.putAll(extractResult(response.item()));
        logDebug(() -> "Result: " + response);
      }
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private Status query(String table, String indexName, Map<String, AttributeValue> key, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var builder = QueryRequest.builder().tableName(table).limit(recordcount);

    if (indexName != null && !indexName.isEmpty()) {
      builder.indexName(indexName);
    }

    if (config.useLegacyAPI()) {
      configureLegacyQuery(builder, key, fields);
    } else {
      configureExpressionQuery(builder, key, fields);
    }

    try {
      var response = dynamoDbClient.query(builder.build()).join();
      if (response.count() > 0 && response.items() != null) {
        response.items().stream().map(this::extractResult).forEach(result::add);
      }
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private void configureLegacyQuery(QueryRequest.Builder builder, Map<String, AttributeValue> key, Set<String> fields) {
    builder.attributesToGet(fields);
    var conditions = key.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Condition.builder().comparisonOperator(ComparisonOperator.EQ).attributeValueList(e.getValue()).build()));
    builder.keyConditions(conditions);
  }

  private void configureExpressionQuery(QueryRequest.Builder builder, Map<String, AttributeValue> key, Set<String> fields) {
    var attrNames = (fields != null && !fields.isEmpty()) ? aliasFields(fields) : new HashMap<String, String>();
    var attrValues = new HashMap<String, AttributeValue>();
    var keyExpression = new StringBuilder();
    var first = true;

    for (var entry : key.entrySet()) {
      if (!first) {
        keyExpression.append(" AND ");
      }
      var nameAlias = addAlias("#", entry.getKey(), attrNames);
      var valueAlias = addAlias(":", entry.getValue(), attrValues);
      keyExpression.append(nameAlias).append("=").append(valueAlias);
      first = false;
    }

    if (fields != null && !fields.isEmpty()) {
      var projectionKeys = attrNames.entrySet().stream().filter(e -> e.getValue() instanceof String s && fields.contains(s)).map(Map.Entry::getKey).toList();
      if (!projectionKeys.isEmpty()) {
        builder.projectionExpression(String.join(",", projectionKeys));
      }
    }

    builder.expressionAttributeNames(attrNames).expressionAttributeValues(attrValues).keyConditionExpression(keyExpression.toString());
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    return switch (config.primaryKeyType()) {
      case HASH -> Map.of(config.primaryKeyName(), AttributeValue.fromS(key));
      case HASH_AND_RANGE ->
          Map.of(config.hashKeyName(), AttributeValue.fromS(config.hashKeyValue()), config.primaryKeyName(), AttributeValue.fromS(key));
    };
  }

  private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (item == null) {
      return null;
    }
    var result = new HashMap<String, ByteIterator>(item.size());
    item.forEach((k, v) -> {
      logDebug(() -> "Result- key: " + k + ", value: " + v);
      result.put(k, new StringByteIterator(v.s()));
    });
    return result;
  }

  private Map<String, String> aliasFields(Set<String> fields) {
    var aliases = new HashMap<String, String>();
    fields.forEach(f -> addAlias("#", f, aliases));
    return aliases;
  }

  private <V> String addAlias(String prefix, V value, Map<String, V> existing) {
    var alias = prefix + "X" + existing.size();
    existing.put(alias, value);
    return alias;
  }

  private long currentTtl() {
    return (System.currentTimeMillis() / MILLIS_TO_SECONDS) + config.ttlDuration();
  }

  private void logDebug(Supplier<String> message) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(message.get());
    }
  }

  private Status handleAsyncException(CompletionException exception) {
    var cause = exception.getCause() != null ? exception.getCause() : exception;
    if (cause instanceof AwsServiceException awsException) {
      LOGGER.error(awsException);
      return Status.ERROR;
    }
    if (cause instanceof SdkClientException sdkException) {
      LOGGER.error(sdkException);
      return CLIENT_ERROR;
    }
    LOGGER.error(cause);
    return CLIENT_ERROR;
  }
}
