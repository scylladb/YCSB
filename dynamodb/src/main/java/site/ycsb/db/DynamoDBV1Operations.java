/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2026 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */

package site.ycsb.db;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AWS SDK v1 (sync, Apache HTTP) implementation. Selected via {@code dynamodb.sdkVersion=v1}.
 *
 * <p>Restored from the pre-v1.1.0 binding so we can benchmark the v1 sync path against the
 * v2 async/Netty path back-to-back. Alternator load balancing is also supported on this path
 * via {@link AlternatorV1Router} — a small in-binding poller that mirrors the v2 driver's
 * {@code /localnodes} discovery and round-robins per request across one client per node.
 */
final class DynamoDBV1Operations implements DynamoDBOperations {

  private static final Logger LOGGER = Logger.getLogger(DynamoDBV1Operations.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";

  private static final Lock CLIENT_LOCK = new ReentrantLock();
  private static final AtomicInteger CLIENT_REF_COUNT = new AtomicInteger();
  private static volatile AmazonDynamoDB sharedClient;
  private static volatile AlternatorV1Router sharedRouter;

  private enum PrimaryKeyType {HASH, HASH_AND_RANGE}

  // Either {@code dynamoDB} (single endpoint mode) or {@code router} (Alternator LB mode) is set.
  private AmazonDynamoDB dynamoDB;
  private AlternatorV1Router router;
  private boolean useLegacyAPI = false;
  private String primaryKeyName;
  private PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;
  private String hashKeyValue;
  private String hashKeyName;
  private String ttlKeyName;
  private long ttlDuration;
  private boolean consistentRead = false;
  private boolean inclusiveScan = true;

  @Override
  public void init(Properties props) throws DBException {
    if ("true".equalsIgnoreCase(props.getProperty("dynamodb.debug"))) {
      LOGGER.setLevel(Level.DEBUG);
    }

    var primaryKey = props.getProperty("dynamodb.primaryKey");
    if (primaryKey == null || primaryKey.isEmpty()) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }
    this.primaryKeyName = primaryKey;

    var primaryKeyTypeString = props.getProperty("dynamodb.primaryKeyType");
    if (primaryKeyTypeString != null && !primaryKeyTypeString.isEmpty()) {
      try {
        this.primaryKeyType = PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid primary key mode: " + primaryKeyTypeString
            + ". Expected HASH or HASH_AND_RANGE.");
      }
    }
    if (this.primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      var configuredHashKeyName = props.getProperty("dynamodb.hashKeyName");
      if (configuredHashKeyName == null || configuredHashKeyName.isEmpty()) {
        throw new DBException("Must specify a non-empty hash key name when primary key type is HASH_AND_RANGE.");
      }
      this.hashKeyName = configuredHashKeyName;
      this.hashKeyValue = props.getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
    }

    var ttlKey = props.getProperty("dynamodb.ttlKey");
    var ttlDurationStr = props.getProperty("dynamodb.ttlDuration");
    if (ttlKey != null && ttlDurationStr != null) {
      this.ttlKeyName = ttlKey;
      this.ttlDuration = Long.parseLong(ttlDurationStr);
    }

    if ("true".equalsIgnoreCase(props.getProperty("dynamodb.consistentReads"))) {
      this.consistentRead = true;
    }
    if ("false".equalsIgnoreCase(props.getProperty("dynamodb.inclusiveScan"))) {
      this.inclusiveScan = false;
    }
    if ("true".equalsIgnoreCase(props.getProperty("dynamodb.useLegacyAPI"))) {
      this.useLegacyAPI = true;
    }

    var useLoadBalancing = Boolean.parseBoolean(
        props.getProperty("dynamodb.alternator.loadbalancing", "false"));

    CLIENT_LOCK.lock();
    try {
      if (useLoadBalancing) {
        if (sharedRouter == null) {
          sharedRouter = buildRouter(props);
        }
        this.router = sharedRouter;
      } else {
        if (sharedClient == null) {
          sharedClient = buildClient(props);
        }
        this.dynamoDB = sharedClient;
      }
      CLIENT_REF_COUNT.incrementAndGet();
    } finally {
      CLIENT_LOCK.unlock();
    }

    LOGGER.info("dynamodb v1 client initialized, endpoint=" + props.getProperty("dynamodb.endpoint")
        + ", alternator.loadbalancing=" + useLoadBalancing);
  }

  private AlternatorV1Router buildRouter(Properties props) throws DBException {
    var endpoint = props.getProperty("dynamodb.endpoint");
    if (endpoint == null || endpoint.isEmpty()) {
      throw new DBException("dynamodb.alternator.loadbalancing requires dynamodb.endpoint");
    }
    var region = props.getProperty("dynamodb.region", "us-east-1");
    var datacenter = props.getProperty("dynamodb.alternator.datacenter");
    var rack = props.getProperty("dynamodb.alternator.rack");
    var portOverride = parseIntProp(props, "dynamodb.alternator.port", -1);

    var clientConfig = buildClientConfiguration(props);
    AWSCredentialsProvider credentials = loadCredentials(props);

    return new AlternatorV1Router(URI.create(endpoint), portOverride, region, datacenter, rack,
        clientConfig, credentials);
  }

  private AmazonDynamoDB buildClient(Properties props) throws DBException {
    var endpoint = props.getProperty("dynamodb.endpoint");
    var region = props.getProperty("dynamodb.region", "us-east-1");

    var builder = AmazonDynamoDBClientBuilder.standard()
        .withClientConfiguration(buildClientConfiguration(props));
    if (endpoint != null) {
      builder = builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));
    } else {
      builder = builder.withRegion(region);
    }

    var credentials = loadCredentials(props);
    if (credentials != null) {
      builder = builder.withCredentials(credentials);
    }

    try {
      return builder.build();
    } catch (Exception e) {
      throw new DBException("Failed to build SDK v1 DynamoDB client", e);
    }
  }

  private static ClientConfiguration buildClientConfiguration(Properties props) {
    var threadCount = parseIntProp(props, Client.THREAD_COUNT_PROPERTY, 1);
    var maxConnects = parseIntProp(props, "dynamodb.connectMax", Math.max(50, threadCount));

    var clientConfig = new ClientConfiguration()
        .withTcpKeepAlive(true)
        .withMaxConnections(maxConnects);

    var connectionTtlMillis = parseLongProp(props, "dynamodb.connectionTimeToLiveSeconds", 0L) * 1000L;
    if (connectionTtlMillis > 0) {
      clientConfig = clientConfig.withConnectionTTL(connectionTtlMillis);
    }
    var connectionMaxIdleSeconds = parseLongProp(props, "dynamodb.connectionMaxIdleTimeSeconds", -1L);
    if (connectionMaxIdleSeconds >= 0) {
      clientConfig = clientConfig.withConnectionMaxIdleMillis(connectionMaxIdleSeconds * 1000L);
    }

    LOGGER.info("Apache HTTP pool: maxConnections=" + maxConnects
        + ", connectionTtlSeconds=" + (connectionTtlMillis / 1000L)
        + ", connectionMaxIdleSeconds=" + connectionMaxIdleSeconds);
    return clientConfig;
  }

  private AWSStaticCredentialsProvider loadCredentials(Properties props) throws DBException {
    var credentialsFile = props.getProperty("dynamodb.awsCredentialsFile");
    var accessKey = props.getProperty("dynamodb.awsAccessKey", "");
    var secretKey = props.getProperty("dynamodb.awsSecretKey", "");

    if (credentialsFile != null && !credentialsFile.isEmpty()) {
      try {
        return new AWSStaticCredentialsProvider(new PropertiesCredentials(new File(credentialsFile)));
      } catch (Exception e) {
        throw new DBException("Failed to read credentials from: " + credentialsFile, e);
      }
    }
    if (!accessKey.trim().isEmpty() && !secretKey.trim().isEmpty()) {
      return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey.trim(), secretKey.trim()));
    }
    // Fallback: bridge the AWS SDK v2-style credentials surface so a single configuration works
    // for both SDK paths. v2 reads system properties {@code aws.accessKeyId} and
    // {@code aws.secretAccessKey} (and the matching env vars), while v1's built-in chain looks
    // at {@code aws.secretKey} (note: missing "Access"). Without this bridge, setting the
    // standard v2 properties — what the AWS SDK docs and most tooling document — would silently
    // fail when {@code dynamodb.sdkVersion=v1}.
    var bridged = bridgeFromV2Environment();
    if (bridged != null) {
      return new AWSStaticCredentialsProvider(bridged);
    }
    return null;
  }

  private static BasicAWSCredentials bridgeFromV2Environment() {
    // System properties: prefer the v2 names, but also accept the v1 names so explicit v1
    // configurations still work via this same code path.
    var sysAccess = firstNonBlank(System.getProperty("aws.accessKeyId"));
    var sysSecret = firstNonBlank(System.getProperty("aws.secretAccessKey"),
        System.getProperty("aws.secretKey"));
    if (sysAccess != null && sysSecret != null) {
      return new BasicAWSCredentials(sysAccess, sysSecret);
    }
    // Environment variables (same names for v1 and v2).
    var envAccess = firstNonBlank(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_ACCESS_KEY"));
    var envSecret = firstNonBlank(System.getenv("AWS_SECRET_ACCESS_KEY"), System.getenv("AWS_SECRET_KEY"));
    if (envAccess != null && envSecret != null) {
      return new BasicAWSCredentials(envAccess, envSecret);
    }
    return null;
  }

  private static String firstNonBlank(String... values) {
    for (var v : values) {
      if (v != null && !v.isBlank()) {
        return v.trim();
      }
    }
    return null;
  }

  private static int parseIntProp(Properties props, String key, int defaultValue) {
    var raw = props.getProperty(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      var value = Integer.parseInt(raw.trim());
      return value > 0 ? value : defaultValue;
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid " + key + "=" + raw + ", falling back to " + defaultValue);
      return defaultValue;
    }
  }

  private static long parseLongProp(Properties props, String key, long defaultValue) {
    var raw = props.getProperty(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("Invalid " + key + "=" + raw + ", falling back to " + defaultValue);
      return defaultValue;
    }
  }

  @Override
  public void cleanup() {
    CLIENT_LOCK.lock();
    try {
      if (CLIENT_REF_COUNT.decrementAndGet() > 0) {
        return;
      }
      if (sharedClient != null) {
        try {
          sharedClient.shutdown();
        } catch (Exception ignored) {
          // best-effort
        }
        sharedClient = null;
      }
      if (sharedRouter != null) {
        try {
          sharedRouter.close();
        } catch (Exception ignored) {
          // best-effort
        }
        sharedRouter = null;
      }
    } finally {
      CLIENT_LOCK.unlock();
    }
  }

  /**
   * Resolves a client for this operation. In single-endpoint mode, always the shared client.
   * In Alternator LB mode, the router round-robins across discovered nodes.
   */
  private AmazonDynamoDB pickClient() {
    return router != null ? router.nextClient() : dynamoDB;
  }

  // ---- Helpers --------------------------------------------------------------

  private String getAlias(String prefix, Map<String, ?> existing) {
    return prefix + "X" + existing.size();
  }

  private <V> String addAlias(String prefix, V field, Map<String, V> existing) {
    var alias = getAlias(prefix, existing);
    existing.put(alias, field);
    return alias;
  }

  private Map<String, String> aliasFields(Set<String> fields, String prefix) {
    var aliasedFields = new HashMap<String, String>();
    for (var field : fields) {
      addAlias(prefix, field, aliasedFields);
    }
    return aliasedFields;
  }

  private long currentTtlEpochSeconds() {
    return (System.currentTimeMillis() / 1000L) + ttlDuration;
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    var k = new HashMap<String, AttributeValue>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else {
      k.put(hashKeyName, new AttributeValue().withS(hashKeyValue));
      k.put(primaryKeyName, new AttributeValue().withS(key));
    }
    return k;
  }

  private String[] splitTableIndex(String table) {
    var parts = table.split(":", 2);
    if (parts.length < 2) {
      return new String[]{parts[0], null};
    }
    return parts;
  }

  private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (item == null) {
      return null;
    }
    var rItems = new HashMap<String, ByteIterator>(item.size());
    for (var attr : item.entrySet()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Result- key: " + attr.getKey() + ", value: " + attr.getValue());
      }
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().getS()));
    }
    return rItems;
  }

  // ---- DB operations --------------------------------------------------------

  private Status getItem(String table, Map<String, AttributeValue> key, Set<String> fields,
                         Map<String, ByteIterator> result, boolean inScan) {
    var req = new GetItemRequest(table, key);
    if (useLegacyAPI) {
      req.setAttributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      var aliases = aliasFields(fields, "#");
      req.setExpressionAttributeNames(aliases);
      req.setProjectionExpression(String.join(",", aliases.keySet()));
    }
    if (!inScan) {
      req.setConsistentRead(consistentRead);
    }
    GetItemResult res;
    try {
      res = pickClient().getItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    if (res.getItem() != null) {
      result.putAll(extractResult(res.getItem()));
      if (!inScan && LOGGER.isDebugEnabled()) {
        LOGGER.debug("Result: " + res);
      }
    }
    return Status.OK;
  }

  private Status query(String table, String indexName, Map<String, AttributeValue> key, int recordcount,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var req = new QueryRequest(table);
    req.setLimit(recordcount);
    if (indexName != null && !indexName.isEmpty()) {
      req.setIndexName(indexName);
    }
    if (useLegacyAPI) {
      req.setAttributesToGet(fields);
      for (var attr : key.entrySet()) {
        req.addKeyConditionsEntry(attr.getKey(), new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(attr.getValue()));
      }
    } else {
      Map<String, String> attrNames;
      if (fields != null && !fields.isEmpty()) {
        attrNames = aliasFields(fields, "#");
        req.setProjectionExpression(String.join(",", attrNames.keySet()));
      } else {
        attrNames = new HashMap<>();
      }
      var attrValues = new HashMap<String, AttributeValue>();
      var keyConditionExpression = new StringBuilder();
      var separator = "";
      for (var attr : key.entrySet()) {
        var nameAlias = addAlias("#", attr.getKey(), attrNames);
        var valueAlias = addAlias(":", attr.getValue(), attrValues);
        keyConditionExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
        separator = " AND ";
      }
      req.setExpressionAttributeNames(attrNames);
      req.setExpressionAttributeValues(attrValues);
      req.setKeyConditionExpression(keyConditionExpression.toString());
    }

    QueryResult queryResult;
    try {
      queryResult = pickClient().query(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    if (queryResult.getCount() > 0 && queryResult.getItems() != null) {
      for (var item : queryResult.getItems()) {
        result.add(extractResult(item));
      }
    }
    return Status.OK;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    var ti = splitTableIndex(table);
    if (ti[1] == null) {
      return getItem(ti[0], createPrimaryKey(key), fields, result, false);
    }
    var tempResult = new Vector<HashMap<String, ByteIterator>>();
    var status = query(ti[0], ti[1], createPrimaryKey(key), 1, fields, tempResult);
    if (status == Status.OK && !tempResult.isEmpty()) {
      result.putAll(tempResult.get(0));
    }
    return status;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    var ti = splitTableIndex(table);
    var tableName = ti[0];

    Map<String, AttributeValue> startKey = (startkey == null || startkey.isEmpty())
        ? null : createPrimaryKey(startkey);
    var count = 0;

    if (startKey != null && this.inclusiveScan) {
      Status start;
      if (ti[1] == null) {
        var tempResult = new HashMap<String, ByteIterator>();
        start = getItem(tableName, startKey, fields, tempResult, true);
        if (!tempResult.isEmpty()) {
          result.add(tempResult);
          count = 1;
        }
      } else {
        var tempResult = new Vector<HashMap<String, ByteIterator>>();
        start = query(tableName, ti[1], startKey, recordcount, fields, tempResult);
        if (!tempResult.isEmpty()) {
          result.addAll(tempResult);
          count = tempResult.size();
        }
      }
      if (start != Status.OK) {
        return start;
      }
    }

    var req = new ScanRequest(tableName);
    if (ti[1] != null && !ti[1].isEmpty()) {
      req.setIndexName(ti[1]);
    }
    if (useLegacyAPI) {
      req.setAttributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      var aliases = aliasFields(fields, "#");
      req.setExpressionAttributeNames(aliases);
      req.setProjectionExpression(String.join(",", aliases.keySet()));
    }

    while (count < recordcount) {
      if (startKey != null) {
        req.setExclusiveStartKey(startKey);
      }
      req.setLimit(recordcount - count);
      ScanResult res;
      try {
        res = pickClient().scan(req);
      } catch (AmazonServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (AmazonClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }
      count += res.getCount();
      for (var items : res.getItems()) {
        result.add(extractResult(items));
      }
      startKey = res.getLastEvaluatedKey();
      if (startKey == null) {
        break;
      }
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    var tableName = splitTableIndex(table)[0];
    var req = new UpdateItemRequest()
        .withTableName(tableName)
        .withKey(createPrimaryKey(key));

    if (useLegacyAPI) {
      var attributes = new HashMap<String, AttributeValueUpdate>(values.size());
      for (var val : values.entrySet()) {
        attributes.put(val.getKey(), new AttributeValueUpdate()
            .withValue(new AttributeValue(val.getValue().toString())).withAction("PUT"));
      }
      if (ttlKeyName != null) {
        attributes.put(ttlKeyName, new AttributeValueUpdate()
            .withValue(new AttributeValue().withN(String.valueOf(currentTtlEpochSeconds())))
            .withAction("PUT"));
      }
      req.setAttributeUpdates(attributes);
    } else {
      var attrNames = new HashMap<String, String>();
      var attrValues = new HashMap<String, AttributeValue>();
      var updateExpression = new StringBuilder();
      var separator = "SET ";
      for (Entry<String, ByteIterator> val : values.entrySet()) {
        var v = new AttributeValue(val.getValue().toString());
        var nameAlias = addAlias("#", val.getKey(), attrNames);
        var valueAlias = addAlias(":", v, attrValues);
        updateExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
        separator = ",";
      }
      if (ttlKeyName != null) {
        var v = new AttributeValue().withN(String.valueOf(currentTtlEpochSeconds()));
        var nameAlias = addAlias("#", ttlKeyName, attrNames);
        var valueAlias = addAlias(":", v, attrValues);
        updateExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
      }
      req.setExpressionAttributeNames(attrNames);
      req.setExpressionAttributeValues(attrValues);
      req.setUpdateExpression(updateExpression.toString());
    }

    try {
      pickClient().updateItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    var tableName = splitTableIndex(table)[0];
    var attributes = new HashMap<String, AttributeValue>(values.size() + 1);
    for (var val : values.entrySet()) {
      attributes.put(val.getKey(), new AttributeValue(val.getValue().toString()));
    }
    attributes.put(primaryKeyName, new AttributeValue(key));
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      attributes.put(hashKeyName, new AttributeValue(hashKeyValue));
    }
    if (ttlKeyName != null) {
      attributes.put(ttlKeyName, new AttributeValue().withN(String.valueOf(currentTtlEpochSeconds())));
    }
    try {
      pickClient().putItem(new PutItemRequest(tableName, attributes));
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    var tableName = splitTableIndex(table)[0];
    try {
      pickClient().deleteItem(new DeleteItemRequest(tableName, createPrimaryKey(key)));
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }
}
