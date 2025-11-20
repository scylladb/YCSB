/*
 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Copyright 2015-2016 YCSB Contributors. All Rights Reserved.
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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import site.ycsb.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

/**
 * DynamoDB client for YCSB.
 */

public class DynamoDBClient extends DB {

  /**
   * Defines the primary key type used in this particular DB instance.
   * <p>
   * By default, the primary key type is "HASH". Optionally, the user can
   * choose to use hash_and_range key type. See documentation in the
   * DynamoDB.Properties file for more details.
   */
  private enum PrimaryKeyType {
    HASH,
    HASH_AND_RANGE
  }

  private AmazonDynamoDB dynamoDB;
  private boolean useLegacyAPI = false;
  private String primaryKeyName;
  private PrimaryKeyType primaryKeyType = PrimaryKeyType.HASH;

  // If the user choose to use HASH_AND_RANGE as primary key type, then
  // the following two variables become relevant. See documentation in the
  // DynamoDB.Properties file for more details.
  private String hashKeyValue;
  private String hashKeyName;
  private String ttlKeyName = null;
  private long ttlDuration;

  private boolean consistentRead = false;
  private boolean inclusiveScan = true;
  private String region = "us-east-1";
  private String endpoint = null;
  private int maxConnects = 50;
  private static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("dynamodb.debug", null);

    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String configuredEndpoint = getProperties().getProperty("dynamodb.endpoint", null);
    String credentialsFile = getProperties().getProperty("dynamodb.awsCredentialsFile", null);
    String primaryKey = getProperties().getProperty("dynamodb.primaryKey", null);
    String ttlKeyConfiguration = getProperties().getProperty("dynamodb.ttlKey", null);
    String ttlDurationConfiguration = getProperties().getProperty("dynamodb.ttlDuration", null);
    String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", null);
    String consistentReads = getProperties().getProperty("dynamodb.consistentReads", null);
    String inclusiveScans = getProperties().getProperty("dynamodb.inclusiveScan", null);
    String connectMax = getProperties().getProperty("dynamodb.connectMax", null);
    String configuredRegion = getProperties().getProperty("dynamodb.region", null);
    String useLegacy = getProperties().getProperty("dynamodb.useLegacyAPI", null);

    if (null != connectMax) {
      this.maxConnects = Integer.parseInt(connectMax);
    }

    if (null != consistentReads && "true".equalsIgnoreCase(consistentReads)) {
      this.consistentRead = true;
    }

    if (null != inclusiveScans && "false".equalsIgnoreCase(inclusiveScans)) {
      this.inclusiveScan = false;
    }

    if (null != useLegacy && "true".equalsIgnoreCase(useLegacy)) {
      this.useLegacyAPI = true;
    }

    if (null != configuredEndpoint) {
      this.endpoint = configuredEndpoint;
    }

    if (null != ttlKeyConfiguration && null != ttlDurationConfiguration) {
      this.ttlKeyName = ttlKeyConfiguration;
      this.ttlDuration = Integer.parseInt(ttlDurationConfiguration);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("ttlKeyName: " + this.ttlKeyName + " ttlDuration: " + this.ttlDuration);
      }
    }

    if (null == primaryKey || primaryKey.length() < 1) {
      throw new DBException("Missing primary key attribute name, cannot continue");
    }

    if (null != primaryKeyTypeString) {
      try {
        this.primaryKeyType = PrimaryKeyType.valueOf(primaryKeyTypeString.trim().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new DBException("Invalid primary key mode specified: " + primaryKeyTypeString +
            ". Expecting HASH or HASH_AND_RANGE.");
      }
    }

    if (this.primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // When the primary key type is HASH_AND_RANGE, keys used by YCSB
      // are range keys so we can benchmark performance of individual hash
      // partitions. In this case, the user must specify the hash key's name
      // and optionally can designate a value for the hash key.

      String configuredHashKeyName = getProperties().getProperty("dynamodb.hashKeyName", null);
      if (null == configuredHashKeyName || configuredHashKeyName.isEmpty()) {
        throw new DBException("Must specify a non-empty hash key name when the primary key type is HASH_AND_RANGE.");
      }
      this.hashKeyName = configuredHashKeyName;
      this.hashKeyValue = getProperties().getProperty("dynamodb.hashKeyValue", DEFAULT_HASH_KEY_VALUE);
    }

    if (null != configuredRegion && configuredRegion.length() > 0) {
      region = configuredRegion;
    }

    try {
      AmazonDynamoDBClientBuilder dynamoDBBuilder = AmazonDynamoDBClientBuilder.standard();
      dynamoDBBuilder = null == endpoint ?
          dynamoDBBuilder.withRegion(this.region) :
          dynamoDBBuilder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(this.endpoint, this.region)
          );
      dynamoDB = dynamoDBBuilder
          .withClientConfiguration(
              new ClientConfiguration()
                  .withTcpKeepAlive(true)
                  .withMaxConnections(this.maxConnects)
          )
          .withCredentials(new AWSStaticCredentialsProvider(new PropertiesCredentials(new File(credentialsFile))))
          .build();
      primaryKeyName = primaryKey;
      LOGGER.info("dynamodb connection created with " + this.endpoint);
    } catch (Exception e1) {
      LOGGER.error("DynamoDBClient.init(): Could not initialize DynamoDB client.", e1);
    }
  }

  // DynamoDB has reserved words and signs, so lets alias all fields
  private String getAlias(String prefix, Map<String, ?> existing) {
    return prefix + "X" + existing.size();
  }
  private <V> String addAlias(String prefix, V field, Map<String, V> existing) {
    String alias = getAlias(prefix, existing);
    existing.put(alias, field);
    return alias;
  }
  private Map<String, String> aliasFields(Set<String> fields, String prefix) {
    Map<String, String> aliasedFields = new HashMap<>();
    for (String field : fields) {
      addAlias(prefix, field, aliasedFields);
    }
    return aliasedFields;
  }

  private Status getItem(String table, Map<String, AttributeValue> key, Set<String> fields,
                         Map<String, ByteIterator> result, boolean inScan) {
    GetItemRequest req = new GetItemRequest(table, key);
    if (useLegacyAPI) {
      req.setAttributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      Map<String, String> aliases = aliasFields(fields, "#");
      req.setExpressionAttributeNames(aliases);
      req.setProjectionExpression(String.join(",", aliases.keySet()));
    }
    if (!inScan) {
      req.setConsistentRead(consistentRead);
    }
    GetItemResult res;

    try {
      res = dynamoDB.getItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (null != res.getItem()) {
      result.putAll(extractResult(res.getItem()));
      if (!inScan && LOGGER.isDebugEnabled()) {
        LOGGER.debug("Result: " + res.toString());
      }
    }

    return Status.OK;
  }
  private Status getItem(String table, Map<String, AttributeValue> key, Set<String> fields,
                         Map<String, ByteIterator> result) {
    return getItem(table, key, fields, result, false);
  }

  private Status query(String table, String indexName, Map<String, AttributeValue> key, int recordcount,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    QueryRequest queryRequest = new QueryRequest(table);
    queryRequest.setLimit(recordcount);
    if (!indexName.isEmpty()) {
      queryRequest.setIndexName(indexName);
    }
    
    if (useLegacyAPI) {
      queryRequest.setAttributesToGet(fields);
      for (Map.Entry<String, AttributeValue> attr : key.entrySet()) {
        Condition keycondition = new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(attr.getValue());
        queryRequest.addKeyConditionsEntry(attr.getKey(), keycondition);
      }
    } else {
      Map<String, String> attrNames;
      if (fields != null && !fields.isEmpty()) {
        attrNames = aliasFields(fields, "#");
        queryRequest.setProjectionExpression(String.join(",", attrNames.keySet()));
      } else {
        attrNames = new HashMap<>();
      }
      Map<String, AttributeValue> attrValues = new HashMap<>();
      StringBuilder keyConditionExpression = new StringBuilder();
      String separator = "";
      for (Entry<String, AttributeValue> attr : key.entrySet()) {
        String nameAlias = addAlias("#", attr.getKey(), attrNames);
        String valueAlias = addAlias(":", attr.getValue(), attrValues);
        keyConditionExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
        separator = " AND ";
      }
      queryRequest.setExpressionAttributeNames(attrNames);
      queryRequest.setExpressionAttributeValues(attrValues);
      queryRequest.setKeyConditionExpression(keyConditionExpression.toString());
    }

    QueryResult queryResult;
    try {
      queryResult = dynamoDB.query(queryRequest);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (queryResult.getCount() > 0 && queryResult.getItems() != null) {
      for (Map<String, AttributeValue> item : queryResult.getItems()) {
        result.add(extractResult(item));
      }
    }
    return Status.OK;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    String[] tableIndex = splitTableIndex(table);
    if (tableIndex[1] == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("readkey: " + key + " from table: " + tableIndex[0]);
      }
      return getItem(tableIndex[0], createPrimaryKey(key), fields, result);
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("readkey: " + key + " from table: " + tableIndex[0] + " with index: " + tableIndex[1]);
      }
      Vector<HashMap<String, ByteIterator>> tempResult = new Vector<>();
      Status ret = query(tableIndex[0], tableIndex[1], createPrimaryKey(key), 1, fields, tempResult);
      if (ret == Status.OK && !tempResult.isEmpty()) {
        result.putAll(tempResult.get(0));
      }
      return ret;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String[] tableIndex = splitTableIndex(table);
    table = tableIndex[0];

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
    }

    Map<String, AttributeValue> startKey = (startkey == null || startkey.isEmpty()) ? null : createPrimaryKey(startkey);
    int count = 0;
    if (startKey != null && this.inclusiveScan) {
      /*
      * on DynamoDB's scan, startkey is *exclusive* so we need to
      * fetch 'startKey' and then use scan for the rest
      */
      Status start = Status.OK;
      if (tableIndex[1] == null) {
        HashMap<String, ByteIterator> tempResult = new HashMap<>();
        start = getItem(table, startKey, fields, tempResult, true);
        if (!tempResult.isEmpty()) {
          result.add(tempResult);
          count = 1;
        }
      } else {
        Vector<HashMap<String, ByteIterator>> tempResult = new Vector<>();
        start = query(tableIndex[0], tableIndex[1], startKey, recordcount, fields, tempResult);
        if (!tempResult.isEmpty()) {
          result.addAll(tempResult);
          count = tempResult.size();
        }
      }
      if (start != Status.OK) {
        return start;
      }
      // startKey is done, rest to go.
    }

    ScanRequest req = new ScanRequest(table);
    if (tableIndex[1] != null && !tableIndex[1].isEmpty()) {
      req.setIndexName(tableIndex[1]);
    }
    if (useLegacyAPI) {
      req.setAttributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      Map<String, String> aliases = aliasFields(fields, "#");
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
        res = dynamoDB.scan(req);
      } catch (AmazonServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (AmazonClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }

      count += res.getCount();
      for (Map<String, AttributeValue> items : res.getItems()) {
        result.add(extractResult(items));
      }
      startKey = res.getLastEvaluatedKey();
      if (null == startKey) {
        break;
      }
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    table = splitTableIndex(table)[0];
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: " + key + " from table: " + table);
    }

    UpdateItemRequest req = new UpdateItemRequest()
        .withTableName(table)
        .withKey(createPrimaryKey(key));

    if (useLegacyAPI) {
      Map<String, AttributeValueUpdate> attributes = new HashMap<>(values.size());
      for (Entry<String, ByteIterator> val : values.entrySet()) {
        AttributeValue v = new AttributeValue(val.getValue().toString());
        attributes.put(val.getKey(), new AttributeValueUpdate().withValue(v).withAction("PUT"));
      }

      if (null != this.ttlKeyName) {
        AttributeValue v = new AttributeValue().withN(
            String.valueOf((System.currentTimeMillis() / 1000L) + this.ttlDuration));
        attributes.put(this.ttlKeyName, new AttributeValueUpdate().withValue(v).withAction("PUT"));
      }
      req.setAttributeUpdates(attributes);
    } else {
      Map<String, String> attrNames = new HashMap<>();
      Map<String, AttributeValue> attrValues = new HashMap<>();
      StringBuilder updateExpression = new StringBuilder();
      String separator = "SET ";
      for (Entry<String, ByteIterator> val : values.entrySet()) {
        AttributeValue v = new AttributeValue(val.getValue().toString());
        String nameAlias = addAlias("#", val.getKey(), attrNames);
        String valueAlias = addAlias(":", v, attrValues);
        updateExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
        separator = ",";
      }
      if (null != this.ttlKeyName) {
        AttributeValue v = new AttributeValue().withN(
            String.valueOf((System.currentTimeMillis() / 1000L) + this.ttlDuration));
        String nameAlias = addAlias("#", this.ttlKeyName, attrNames);
        String valueAlias = addAlias(":", v, attrValues);
        updateExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
      }
      req.setExpressionAttributeNames(attrNames);
      req.setExpressionAttributeValues(attrValues);
      req.setUpdateExpression(updateExpression.toString());
    }

    try {
      dynamoDB.updateItem(req);
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
    table = splitTableIndex(table)[0];
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
    }

    Map<String, AttributeValue> attributes = createAttributes(values);
    // adding primary key
    attributes.put(primaryKeyName, new AttributeValue(key));
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // If the primary key type is HASH_AND_RANGE, then what has been put
      // into the attributes map above is the range key part of the primary
      // key, we still need to put in the hash key part here.
      attributes.put(hashKeyName, new AttributeValue(hashKeyValue));
    }

    if (null != this.ttlKeyName) {
      attributes.put(this.ttlKeyName, new AttributeValue().withN(
          String.valueOf((System.currentTimeMillis() / 1000L) + this.ttlDuration)));
    }

    PutItemRequest putItemRequest = new PutItemRequest(table, attributes);
    try {
      dynamoDB.putItem(putItemRequest);
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
    table = splitTableIndex(table)[0];
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }

    DeleteItemRequest req = new DeleteItemRequest(table, createPrimaryKey(key));

    try {
      dynamoDB.deleteItem(req);
    } catch (AmazonServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (AmazonClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  private static Map<String, AttributeValue> createAttributes(Map<String, ByteIterator> values) {
    Map<String, AttributeValue> attributes = new HashMap<>(values.size() + 1);
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      attributes.put(val.getKey(), new AttributeValue(val.getValue().toString()));
    }
    return attributes;
  }

  private HashMap<String, ByteIterator> extractResult(Map<String, AttributeValue> item) {
    if (null == item) {
      return null;
    }
    HashMap<String, ByteIterator> rItems = new HashMap<>(item.size());

    for (Entry<String, AttributeValue> attr : item.entrySet()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("Result- key: %s, value: %s", attr.getKey(), attr.getValue()));
      }
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().getS()));
    }
    return rItems;
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    Map<String, AttributeValue> k = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      k.put(hashKeyName, new AttributeValue().withS(hashKeyValue));
      k.put(primaryKeyName, new AttributeValue().withS(key));
    } else {
      throw new RuntimeException("Assertion Error: impossible primary key type");
    }
    return k;
  }

  private String[] splitTableIndex(String table) {
    String[] parts = table.split(":", 2);
    if (parts.length < 2) {
      return new String[]{parts[0], null};
    }
    return parts;
  }
}
