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
import com.scylladb.alternator.HttpClientType;
import com.scylladb.alternator.TlsConfig;
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
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * DynamoDB client for YCSB.
 *
 * <p>Supports both AWS DynamoDB and ScyllaDB Alternator with client-side load balancing.
 *
 * <h3>Traffic distribution across Alternator nodes (standard path)</h3>
 * When {@code dynamodb.endpoint} is set and Alternator load balancing is disabled, this
 * binding builds a single {@code DynamoDbAsyncClient} whose {@code endpointOverride} is the
 * configured hostname (not an IP). Netty's connection pool is sized to {@code threadcount}
 * so it opens that many parallel HTTP connections; each new connection performs its own
 * DNS lookup, and with the JVM DNS cache TTL forced to {@code 0} the OS resolver returns a
 * different round-robin IP for each new connection. YCSB threads then share that pool of
 * connections, which spreads requests evenly across cluster nodes — the same model the
 * binding used in versions 0.2.x / 1.1.x / 1.2.x, where this was reported as working.
 *
 * <p>This deliberately replaces the per-IP client pool that lived briefly in 1.3.x: forcing
 * one client per resolved IP gave a perfectly even <em>coordinator</em> distribution but
 * pinned every connection to a single IP and bypassed the SDK's own DNS handling, which
 * showed up as observed imbalance on the server side.
 *
 * <h3>Alternator load-balancing path</h3>
 * When {@code dynamodb.alternator.loadbalancing=true}, the binding hands off to
 * {@link AlternatorDynamoDbAsyncClient}, which discovers nodes via {@code /localnodes} and
 * routes per-request. Netty is forced via {@link HttpClientType#NETTY} so both code paths
 * share one async HTTP stack.
 */
public final class DynamoDBClient extends DB {

  static {
    // Disable the JVM's positive DNS cache so each new Netty connection re-resolves the
    // hostname and picks up the next IP from the OS resolver's round-robin rotation. This
    // must run before {@link sun.net.InetAddressCachePolicy} is statically initialized
    // (which happens on the first DNS lookup), so we do it in this class's static block —
    // i.e. as soon as anything references DynamoDBClient. Mirrored in initializeSharedClient
    // for redundancy.
    java.security.Security.setProperty("networkaddress.cache.ttl", "0");
    java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
  }

  private static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";
  private static final long MILLIS_TO_SECONDS = 1000L;
  private static final Lock CLIENT_LOCK = new ReentrantLock();
  private static final java.util.concurrent.atomic.AtomicInteger CLIENT_REF_COUNT =
      new java.util.concurrent.atomic.AtomicInteger();

  private static volatile DynamoDbAsyncClient sharedClient;
  private static volatile ExecutorService sharedCompletionExecutor;

  /**
   * Per-instance reference captured from {@link #sharedClient} during {@link #init()} so
   * cleanup can null the static without affecting in-flight operations on this instance.
   */
  private DynamoDbAsyncClient client;
  private DynamoDBConfig config;

  // ---- Configuration record -------------------------------------------------

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

      return new DynamoDBConfig(primaryKey, primaryKeyType, hashKeyName, hashKeyValue,
          ttlKeyName, ttlDuration,
          Boolean.parseBoolean(props.getProperty("dynamodb.consistentReads", "false")),
          !"false".equalsIgnoreCase(props.getProperty("dynamodb.inclusiveScan")),
          Boolean.parseBoolean(props.getProperty("dynamodb.useLegacyAPI", "false")));
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

  // ---- Lifecycle ------------------------------------------------------------

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
      this.client = sharedClient;
      CLIENT_REF_COUNT.incrementAndGet();
    } finally {
      CLIENT_LOCK.unlock();
    }

    logDebug(() -> config.ttlKeyName() != null
        ? "TTL configured: key=" + config.ttlKeyName() + ", duration=" + config.ttlDuration()
        : "No TTL configured");
  }

  private void initializeSharedClient(java.util.Properties props) {
    // Force the JVM DNS cache TTL to 0 so each new Netty connection re-resolves the hostname
    // and picks up the next IP from the OS resolver's round-robin rotation. Without this the
    // JVM happily caches the first resolution forever and every connection lands on one IP.
    java.security.Security.setProperty("networkaddress.cache.ttl", "0");

    var endpoint = props.getProperty("dynamodb.endpoint");
    LOGGER.info("DynamoDB endpoint: " + endpoint);

    var useLoadBalancing = Boolean.parseBoolean(
        props.getProperty("dynamodb.alternator.loadbalancing", "false"));
    LOGGER.info("Alternator load balancing: " + useLoadBalancing);

    var region = Optional.ofNullable(props.getProperty("dynamodb.region"))
        .map(Region::of).orElse(Region.US_EAST_1);

    if (useLoadBalancing && endpoint != null) {
      var alternatorBuilder = AlternatorDynamoDbAsyncClient.builder();
      alternatorBuilder.region(region);
      alternatorBuilder.withAlternatorConfig(createAlternatorConfig(props, endpoint));
      alternatorBuilder.endpointOverride(URI.create(endpoint));
      // Force Netty so the AWS SDK and the Alternator load balancer share one async HTTP stack.
      // Mutually exclusive with httpClient()/httpClientBuilder(); we don't set those here.
      alternatorBuilder.withHttpClientType(HttpClientType.NETTY);
      LOGGER.info("Alternator LB seed: " + endpoint);

      configureCredentials(alternatorBuilder, props);
      configureVirtualThreads(alternatorBuilder, props);
      configureInterceptors(alternatorBuilder, props);

      sharedClient = alternatorBuilder.build();
      return;
    }

    // Standard path: one async client targeting the hostname. Netty opens up to threadcount
    // connections in parallel, each resolving DNS on its own — that's where the spread comes
    // from. Equivalent to the v0.2.0 / v1.1.0 / v1.2.0 setup that the user observed working.
    var standardBuilder = DynamoDbAsyncClient.builder().region(region);
    if (endpoint != null) {
      standardBuilder.endpointOverride(URI.create(endpoint));
    }
    configureCredentials(standardBuilder, props);
    configureHttpClient(standardBuilder, props);
    configureVirtualThreads(standardBuilder, props);
    configureInterceptors(standardBuilder, props);
    sharedClient = standardBuilder.build();
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
      if (sharedClient != null) {
        sharedClient.close();
        sharedClient = null;
      }
    } finally {
      CLIENT_LOCK.unlock();
    }
  }

  // ---- Client selection -----------------------------------------------------

  private DynamoDbAsyncClient nextClient() {
    return client;
  }

  // ---- Configuration helpers ------------------------------------------------

  private void configureCredentials(software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder builder,
                                    java.util.Properties props) {
    var credentials = loadCredentials(props);
    if (credentials != null) {
      builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
    }
  }

  private AwsCredentials loadCredentials(java.util.Properties props) {
    var accessKey = props.getProperty("dynamodb.awsAccessKey", "");
    var secretKey = props.getProperty("dynamodb.awsSecretKey", "");
    var credentialsFile = props.getProperty("dynamodb.awsCredentialsFile");

    if (credentialsFile != null && !credentialsFile.isEmpty()) {
      return loadCredentialsFromFile(credentialsFile);
    } else if (!accessKey.trim().isEmpty() && !secretKey.trim().isEmpty()) {
      return AwsBasicCredentials.create(accessKey.trim(), secretKey.trim());
    }
    return null;
  }

  private void configureHttpClient(software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder builder,
                                   java.util.Properties props) {
    var threadCount = Integer.parseInt(props.getProperty(Client.THREAD_COUNT_PROPERTY, "1"));
    var trustAllCerts = Boolean.parseBoolean(
        props.getProperty("dynamodb.alternator.trustAllCertificates", "false"));

    if (threadCount > 1 || trustAllCerts) {
      var httpClientBuilder = NettyNioAsyncHttpClient.builder()
          .maxConcurrency(threadCount)
          .useNonBlockingDnsResolver(false);

      if (trustAllCerts) {
        LOGGER.warn("Trust all certificates enabled — testing only, never use in production!");
        httpClientBuilder.tlsTrustManagersProvider(createTrustAllProvider());
      }

      builder.httpClientBuilder(httpClientBuilder);
    }
  }

  private void configureVirtualThreads(software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder builder,
                                       java.util.Properties props) {
    if (Boolean.parseBoolean(props.getProperty("dynamodb.virtualThreads", "false"))) {
      sharedCompletionExecutor = Executors.newVirtualThreadPerTaskExecutor();
      builder.asyncConfiguration(ClientAsyncConfiguration.builder()
          .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, sharedCompletionExecutor)
          .build());
    }
  }

  private void configureInterceptors(software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder builder,
                                     java.util.Properties props) {
    var interceptors = props.getProperty("dynamodb.executionInterceptors");
    if (interceptors == null || interceptors.isEmpty()) {
      return;
    }
    var cfg = software.amazon.awssdk.core.client.config.ClientOverrideConfiguration.builder();
    for (var name : interceptors.split(",")) {
      var cn = name.trim();
      if (cn.isEmpty()) {
        continue;
      }
      try {
        var clazz = Class.forName(cn);
        var interceptor = (software.amazon.awssdk.core.interceptor.ExecutionInterceptor)
            clazz.getDeclaredConstructor().newInstance();
        cfg.addExecutionInterceptor(interceptor);
      } catch (Throwable t) {
        LOGGER.warn("Failed to add execution interceptor: " + cn, t);
      }
    }
    builder.overrideConfiguration(cfg.build());
  }

  private AlternatorConfig createAlternatorConfig(java.util.Properties props, String endpoint) {
    String datacenter = props.getProperty("dynamodb.alternator.datacenter");
    String rack = props.getProperty("dynamodb.alternator.rack");

    RoutingScope scope;
    if (datacenter != null && rack != null) {
      scope = RackScope.of(datacenter, rack, DatacenterScope.of(datacenter, ClusterScope.create()));
    } else if (datacenter != null) {
      scope = DatacenterScope.of(datacenter, ClusterScope.create());
    } else {
      scope = ClusterScope.create();
    }

    return AlternatorConfig.builder()
        .withSeedNode(URI.create(endpoint))
        .withRoutingScope(scope)
        .withTlsConfig(TlsConfig.systemDefault())
        .withPort(Integer.parseInt(props.getProperty("dynamodb.alternator.port", "-1")))
        .build();
  }

  private AwsCredentials loadCredentialsFromFile(String filePath) {
    try {
      var props = new Properties();
      try (var input = new java.io.FileInputStream(filePath)) {
        props.load(input);
      }
      var accessKey = props.getProperty("accessKey");
      var secretKey = props.getProperty("secretKey");
      if (accessKey != null && !accessKey.trim().isEmpty()
          && secretKey != null && !secretKey.trim().isEmpty()) {
        LOGGER.info("Loaded AWS credentials from: " + filePath);
        return AwsBasicCredentials.create(accessKey.trim(), secretKey.trim());
      }
      return null;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read credentials from: " + filePath, e);
    }
  }

  private TlsTrustManagersProvider createTrustAllProvider() {
    return () -> new TrustManager[]{new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] chain, String authType) { }
      @Override
      public void checkServerTrusted(X509Certificate[] chain, String authType) { }
      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
      }
    }};
  }

  // ---- DB operations --------------------------------------------------------

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    var tableIndex = TableIndex.parse(table);
    return tableIndex.index()
        .map(idx -> readWithIndex(tableIndex.table(), idx, key, fields, result))
        .orElseGet(() -> {
          logDebug(() -> "readkey: " + key + " from table: " + tableIndex.table());
          return getItem(tableIndex.table(), createPrimaryKey(key), fields, result, false);
        });
  }

  private Status readWithIndex(String table, String index, String key,
                               Set<String> fields, Map<String, ByteIterator> result) {
    logDebug(() -> "readkey: " + key + " from table: " + table + " with index: " + index);
    var tempResult = new Vector<HashMap<String, ByteIterator>>();
    var status = query(table, index, createPrimaryKey(key), 1, fields, tempResult);
    if (status == Status.OK && !tempResult.isEmpty()) {
      result.putAll(tempResult.getFirst());
    }
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var tableIndex = TableIndex.parse(table);
    logDebug(() -> "scan " + recordcount + " records from key: " + startkey
        + " on table: " + tableIndex.table());

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

  private InclusiveScanResult handleInclusiveScan(TableIndex tableIndex,
                                                  Map<String, AttributeValue> startKey,
                                                  int recordcount, Set<String> fields,
                                                  Vector<HashMap<String, ByteIterator>> result) {
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

  private Status performScan(TableIndex tableIndex, Map<String, AttributeValue> startKey,
                             int recordcount, int initialCount,
                             Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var scanBuilder = ScanRequest.builder().tableName(tableIndex.table());
    tableIndex.index().ifPresent(scanBuilder::indexName);
    configureProjection(scanBuilder, fields);

    var count = initialCount;
    var currentStartKey = startKey;
    var scanClient = nextClient();

    while (count < recordcount) {
      if (currentStartKey != null) {
        scanBuilder.exclusiveStartKey(currentStartKey);
      }
      scanBuilder.limit(recordcount - count);

      try {
        var response = scanClient.scan(scanBuilder.build()).join();
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
      builder.expressionAttributeNames(aliases)
          .projectionExpression(String.join(",", aliases.keySet()));
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
      nextClient().updateItem(updateBuilder.build()).join();
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private Map<String, AttributeValueUpdate> createLegacyUpdates(Map<String, ByteIterator> values) {
    var updates = new HashMap<String, AttributeValueUpdate>(values.size() + 1);
    values.forEach((k, v) -> updates.put(k,
        AttributeValueUpdate.builder()
            .action(AttributeAction.PUT)
            .value(AttributeValue.fromS(v.toString()))
            .build()));
    if (config.ttlKeyName() != null) {
      updates.put(config.ttlKeyName(), AttributeValueUpdate.builder()
          .action(AttributeAction.PUT)
          .value(AttributeValue.fromN(String.valueOf(currentTtl())))
          .build());
    }
    return updates;
  }

  private void configureExpressionUpdate(UpdateItemRequest.Builder builder,
                                         Map<String, ByteIterator> values) {
    var attrNames = new HashMap<String, String>();
    var attrValues = new HashMap<String, AttributeValue>();
    var expression = new StringBuilder("SET ");
    var first = true;

    for (var entry : values.entrySet()) {
      if (!first) {
        expression.append(",");
      }
      expression.append(addAlias("#", entry.getKey(), attrNames))
          .append("=")
          .append(addAlias(":", AttributeValue.fromS(entry.getValue().toString()), attrValues));
      first = false;
    }

    if (config.ttlKeyName() != null) {
      if (!first) {
        expression.append(",");
      }
      expression.append(addAlias("#", config.ttlKeyName(), attrNames))
          .append("=")
          .append(addAlias(":", AttributeValue.fromN(String.valueOf(currentTtl())), attrValues));
    }

    builder.expressionAttributeNames(attrNames)
        .expressionAttributeValues(attrValues)
        .updateExpression(expression.toString());
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    var tableName = TableIndex.parse(table).table();
    logDebug(() -> "insertkey: " + config.primaryKeyName() + "-" + key + " from table: " + tableName);
    try {
      nextClient().putItem(
          PutItemRequest.builder().item(createInsertAttributes(key, values)).tableName(tableName).build()
      ).join();
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private Map<String, AttributeValue> createInsertAttributes(String key, Map<String, ByteIterator> values) {
    var attributes = values.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            e -> AttributeValue.fromS(e.getValue().toString()),
            (a, b) -> a, HashMap::new));
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
      nextClient().deleteItem(
          DeleteItemRequest.builder().key(createPrimaryKey(key)).tableName(tableName).build()
      ).join();
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  // ---- Private helpers ------------------------------------------------------

  private Status getItem(String table, Map<String, AttributeValue> key, Set<String> fields,
                         Map<String, ByteIterator> result, boolean inScan) {
    var builder = GetItemRequest.builder().key(key).tableName(table);
    if (config.useLegacyAPI()) {
      builder.attributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      var aliases = aliasFields(fields);
      builder.expressionAttributeNames(aliases)
          .projectionExpression(String.join(",", aliases.keySet()));
    }
    if (!inScan) {
      builder.consistentRead(config.consistentRead());
    }
    try {
      var response = nextClient().getItem(builder.build()).join();
      if (response.hasItem()) {
        result.putAll(extractResult(response.item()));
        logDebug(() -> "Result: " + response);
      }
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private Status query(String table, String indexName, Map<String, AttributeValue> key,
                       int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
    var builder = QueryRequest.builder().tableName(table).limit(recordcount);
    if (indexName != null && !indexName.isEmpty()) {
      builder.indexName(indexName);
    }
    if (config.useLegacyAPI()) {
      configureLegacyQuery(builder, key, fields);
    } else {
      configureExpressionQuery(builder, key, fields);
    }
    // Capture once: all pages of this query must go to the same node.
    try {
      var response = nextClient().query(builder.build()).join();
      if (response.count() > 0 && response.items() != null) {
        response.items().stream().map(this::extractResult).forEach(result::add);
      }
      return Status.OK;
    } catch (CompletionException e) {
      return handleAsyncException(e);
    }
  }

  private void configureLegacyQuery(QueryRequest.Builder builder,
                                    Map<String, AttributeValue> key, Set<String> fields) {
    builder.attributesToGet(fields);
    var conditions = key.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            e -> Condition.builder()
                .comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(e.getValue())
                .build()));
    builder.keyConditions(conditions);
  }

  private void configureExpressionQuery(QueryRequest.Builder builder,
                                        Map<String, AttributeValue> key, Set<String> fields) {
    var attrNames = (fields != null && !fields.isEmpty()) ? aliasFields(fields) : new HashMap<String, String>();
    var attrValues = new HashMap<String, AttributeValue>();
    var keyExpression = new StringBuilder();
    var first = true;

    for (var entry : key.entrySet()) {
      if (!first) {
        keyExpression.append(" AND ");
      }
      keyExpression.append(addAlias("#", entry.getKey(), attrNames))
          .append("=")
          .append(addAlias(":", entry.getValue(), attrValues));
      first = false;
    }

    if (fields != null && !fields.isEmpty()) {
      var projectionKeys = attrNames.entrySet().stream()
          .filter(e -> e.getValue() instanceof String s && fields.contains(s))
          .map(Map.Entry::getKey)
          .toList();
      if (!projectionKeys.isEmpty()) {
        builder.projectionExpression(String.join(",", projectionKeys));
      }
    }

    builder.expressionAttributeNames(attrNames)
        .expressionAttributeValues(attrValues)
        .keyConditionExpression(keyExpression.toString());
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    return switch (config.primaryKeyType()) {
      case HASH -> Map.of(config.primaryKeyName(), AttributeValue.fromS(key));
      case HASH_AND_RANGE -> Map.of(
          config.hashKeyName(), AttributeValue.fromS(config.hashKeyValue()),
          config.primaryKeyName(), AttributeValue.fromS(key));
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

  private void logDebug(java.util.function.Supplier<String> message) {
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
      if (sdkException.getCause() instanceof UnknownHostException uhe) {
        LOGGER.error("DNS resolution failed for '" + uhe.getMessage()
            + "': check endpoint configuration and DNS availability");
      } else {
        LOGGER.error(sdkException);
      }
      return CLIENT_ERROR;
    }
    LOGGER.error(cause);
    return CLIENT_ERROR;
  }
}
