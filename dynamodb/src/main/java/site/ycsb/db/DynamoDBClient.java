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

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import site.ycsb.*;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

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

  private volatile DynamoDbClient dynamoDbClient;
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

  private boolean useBatchInsert = false;
  private boolean consistentRead = false;
  private boolean inclusiveScan = true;
  private static final Logger LOGGER = Logger.getLogger(DynamoDBClient.class);
  private static final Status CLIENT_ERROR = new Status("CLIENT_ERROR", "An error occurred on the client.");
  private static final String DEFAULT_HASH_KEY_VALUE = "YCSB_0";
  private static final int DYNAMODB_BATCH_WRITE_MAX_SIZE = 25;
  private final Map<String, List<WriteRequest>> batchInsertItemBuffer = new HashMap<>();

  @Override
  public void init() throws DBException {
    String debug = getProperties().getProperty("dynamodb.debug", null);

    if (null != debug && "true".equalsIgnoreCase(debug)) {
      LOGGER.setLevel(Level.DEBUG);
    }

    String batchInsert = getProperties().getProperty("dynamodb.batchInsert", null);
    String primaryKey = getProperties().getProperty("dynamodb.primaryKey", null);
    String ttlKeyConfiguration = getProperties().getProperty("dynamodb.ttlKey", null);
    String ttlDurationConfiguration = getProperties().getProperty("dynamodb.ttlDuration", null);
    String primaryKeyTypeString = getProperties().getProperty("dynamodb.primaryKeyType", null);
    String consistentReads = getProperties().getProperty("dynamodb.consistentReads", null);
    String inclusiveScans = getProperties().getProperty("dynamodb.inclusiveScan", null);
    String useLegacy = getProperties().getProperty("dynamodb.useLegacyAPI", null);

    if (null != batchInsert && "true".equalsIgnoreCase(batchInsert)) {
      this.useBatchInsert = true;
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
    this.primaryKeyName = primaryKey;

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

    // only create one DynamoDB client, used by all YCSB client threads
    if (dynamoDbClient == null) {
      synchronized (DynamoDbClient.class) {
        if (dynamoDbClient == null) {
          DynamoDbClientBuilder dynamoDbClientBuilder = DynamoDbClient.builder();

          Region region = Region.US_EAST_1;
          String configuredRegion = getProperties().getProperty("dynamodb.region", null);
          if (configuredRegion != null) {
            region = Region.of(configuredRegion);
          }
          dynamoDbClientBuilder.region(region);

          String configuredEndpoint = getProperties().getProperty("dynamodb.endpoint", null);
          if (configuredEndpoint != null) {
            dynamoDbClientBuilder.endpointOverride(URI.create(configuredEndpoint));
          }

          // we create the same number of HTTP threads as there are YCSB threads. YCSB default is "1"
          String configuredThreadCount = getProperties().getProperty(Client.THREAD_COUNT_PROPERTY, "1");

          dynamoDbClientBuilder.httpClient(ApacheHttpClient.builder()
              .maxConnections(Integer.parseInt(configuredThreadCount))
              .tcpKeepAlive(true)
              .build());
          this.dynamoDbClient = dynamoDbClientBuilder.build();
        }
      }
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
    GetItemRequest.Builder getItemBuilder = GetItemRequest.builder()
        .key(key)
        .tableName(table);

    if (useLegacyAPI) {
      getItemBuilder.attributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      Map<String, String> aliases = aliasFields(fields, "#");
      getItemBuilder.expressionAttributeNames(aliases);
      getItemBuilder.projectionExpression(String.join(",", aliases.keySet()));
    }
    if (!inScan) {
      getItemBuilder.consistentRead(consistentRead);
    }

    GetItemResponse res;
    try {
      res = dynamoDbClient.getItem(getItemBuilder.build());
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (res.hasItem()) {
      result.putAll(extractResult(res.item()));
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
    QueryRequest.Builder queryRequestBuilder = QueryRequest.builder()
        .tableName(table)
        .limit(recordcount);
    if (!indexName.isEmpty()) {
      queryRequestBuilder.indexName(indexName);
    }
    
    if (useLegacyAPI) {
      queryRequestBuilder.attributesToGet(fields);
      Map<String, Condition> keyConditions = new HashMap<>();
      for (Map.Entry<String, AttributeValue> attr : key.entrySet()) {
        Condition keycondition = Condition.builder()
            .comparisonOperator(ComparisonOperator.EQ)
            .attributeValueList(attr.getValue()).build();
        keyConditions.put(attr.getKey(), keycondition);
      }
      queryRequestBuilder.keyConditions(keyConditions);
    } else {
      Map<String, String> attrNames;
      if (fields != null && !fields.isEmpty()) {
        attrNames = aliasFields(fields, "#");
        queryRequestBuilder.projectionExpression(String.join(",", attrNames.keySet()));
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
      queryRequestBuilder.expressionAttributeNames(attrNames);
      queryRequestBuilder.expressionAttributeValues(attrValues);
      queryRequestBuilder.keyConditionExpression(keyConditionExpression.toString());
    }

    QueryResponse res;
    try {
      res = dynamoDbClient.query(queryRequestBuilder.build());
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }

    if (res.count() > 0 && res.items() != null) {
      for (Map<String, AttributeValue> item : res.items()) {
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

    ScanRequest.Builder scanRequestBuilder = ScanRequest.builder()
        .tableName(table);
    if (tableIndex[1] != null && !tableIndex[1].isEmpty()) {
      scanRequestBuilder.indexName(tableIndex[1]);
    }
    if (useLegacyAPI) {
      scanRequestBuilder.attributesToGet(fields);
    } else if (fields != null && !fields.isEmpty()) {
      Map<String, String> aliases = aliasFields(fields, "#");
      scanRequestBuilder.expressionAttributeNames(aliases);
      scanRequestBuilder.projectionExpression(String.join(",", aliases.keySet()));
    }
    while (count < recordcount) {
      if (startKey != null) {
        scanRequestBuilder.exclusiveStartKey(startKey);
      }
      scanRequestBuilder.limit(recordcount - count);
      ScanResponse res;
      try {
        res = dynamoDbClient.scan(scanRequestBuilder.build());

      } catch (AwsServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (SdkClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }

      count += res.count();
      for (Map<String, AttributeValue> items : res.items()) {
        result.add(extractResult(items));
      }
      if (!res.hasLastEvaluatedKey()) {
        break;
      }
      startKey = res.lastEvaluatedKey();
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    table = splitTableIndex(table)[0];
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("updatekey: " + key + " from table: " + table);
    }

    UpdateItemRequest.Builder updateItemBuilder = UpdateItemRequest.builder()
        .key(createPrimaryKey(key))
        .tableName(table);

    if (useLegacyAPI) {
      Map<String, AttributeValueUpdate> attributes = new HashMap<>(values.size());
      for (Entry<String, ByteIterator> val : values.entrySet()) {
        AttributeValue v = AttributeValue.fromS(val.getValue().toString());
        attributes.put(val.getKey(), AttributeValueUpdate.builder().action(AttributeAction.PUT).value(v).build());
      }

      if (null != this.ttlKeyName) {
        AttributeValue v = AttributeValue.fromN(
            String.valueOf((System.currentTimeMillis() / 1000L) + this.ttlDuration));
        attributes.put(this.ttlKeyName, AttributeValueUpdate.builder().action(AttributeAction.PUT).value(v).build());
      }
      updateItemBuilder.attributeUpdates(attributes);
    } else {
      Map<String, String> attrNames = new HashMap<>();
      Map<String, AttributeValue> attrValues = new HashMap<>();
      StringBuilder updateExpression = new StringBuilder();
      String separator = "SET ";
      for (Entry<String, ByteIterator> val : values.entrySet()) {
        AttributeValue v = AttributeValue.fromS(val.getValue().toString());
        String nameAlias = addAlias("#", val.getKey(), attrNames);
        String valueAlias = addAlias(":", v, attrValues);
        updateExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
        separator = ",";
      }
      if (null != this.ttlKeyName) {
        AttributeValue v = AttributeValue.fromN(
            String.valueOf((System.currentTimeMillis() / 1000L) + this.ttlDuration));
        String nameAlias = addAlias("#", this.ttlKeyName, attrNames);
        String valueAlias = addAlias(":", v, attrValues);
        updateExpression.append(separator).append(nameAlias).append("=").append(valueAlias);
      }
      updateItemBuilder.expressionAttributeNames(attrNames);
      updateItemBuilder.expressionAttributeValues(attrValues);
      updateItemBuilder.updateExpression(updateExpression.toString());
    }

    try {
      dynamoDbClient.updateItem(updateItemBuilder.build());
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
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
    attributes.put(primaryKeyName, AttributeValue.fromS(key));
    if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      // If the primary key type is HASH_AND_RANGE, then what has been put
      // into the attributes map above is the range key part of the primary
      // key, we still need to put in the hash key part here.
      attributes.put(hashKeyName, AttributeValue.fromS(hashKeyValue));
    }

    if (null != this.ttlKeyName) {
      attributes.put(this.ttlKeyName, AttributeValue.fromN(
          String.valueOf((System.currentTimeMillis() / 1000L) + this.ttlDuration)));
    }
    
    if (useBatchInsert)  {
      WriteRequest writeRequest = WriteRequest.builder()
          .putRequest(PutRequest.builder()
                  .item(attributes)
                  .build()).build();
      if (batchInsertItemBuffer.containsKey(table)) {
        batchInsertItemBuffer.get(table).add(writeRequest);
      } else {
        batchInsertItemBuffer.put(table, new ArrayList<>(Arrays.asList(writeRequest)));
      }
      if (batchInsertItemBuffer.values().stream().mapToInt(List::size).sum() == DYNAMODB_BATCH_WRITE_MAX_SIZE) {
        batchInsertItems();
      }
    } else {
      PutItemRequest putItemRequest = PutItemRequest.builder().item(attributes).tableName(table).build();
      try {
        dynamoDbClient.putItem(putItemRequest);
      } catch (AwsServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (SdkClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }
    }
    return Status.OK;
  }

  private Status batchInsertItems() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("batch write of " + batchInsertItemBuffer.values().stream().mapToInt(List::size).sum() + " items");
    }
    if (batchInsertItemBuffer.values().stream().mapToInt(List::size).sum() > 0) {
      try {
        boolean allItemsInserted = false;
        int writeAttempts = 0;

        // try to put the items in the DynamoDB table
        // retry with an exponential backoff if the DynamoDB client
        // could not process all items successfully
        while (!allItemsInserted && writeAttempts <= 5) {
          BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(BatchWriteItemRequest.builder()
              .requestItems(batchInsertItemBuffer)
              .build());
          writeAttempts++;
          batchInsertItemBuffer.clear();
          if (response.hasUnprocessedItems() &&
              response.unprocessedItems().values().stream().mapToInt(List::size).sum() > 0) {
            batchInsertItemBuffer.putAll(response.unprocessedItems());
            try {
              Thread.sleep(250 * (int)Math.pow(2, writeAttempts));
            } catch (InterruptedException ie) {
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batchWriteItem sleep for exponential backoff & retry got interrupted");
              }
            }
          } else {
            allItemsInserted = true;
          }
        }
        if (!allItemsInserted) {
          LOGGER.error("Client failed to insert all the items in the batch");
          return CLIENT_ERROR;
        }
      } catch (AwsServiceException ex) {
        LOGGER.error(ex);
        return Status.ERROR;
      } catch (SdkClientException ex) {
        LOGGER.error(ex);
        return CLIENT_ERROR;
      }
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    table = splitTableIndex(table)[0];
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("deletekey: " + key + " from table: " + table);
    }

    DeleteItemRequest req = DeleteItemRequest.builder().key(createPrimaryKey(key)).tableName(table).build();

    try {
      dynamoDbClient.deleteItem(req);
    } catch (AwsServiceException ex) {
      LOGGER.error(ex);
      return Status.ERROR;
    } catch (SdkClientException ex) {
      LOGGER.error(ex);
      return CLIENT_ERROR;
    }
    return Status.OK;
  }

  /**
   * Cleanup any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (useBatchInsert) {
      // flush any remaining items to the DynamoDB table
      batchInsertItems();
    }
  }

  private static Map<String, AttributeValue> createAttributes(Map<String, ByteIterator> values) {
    Map<String, AttributeValue> attributes = new HashMap<>(values.size() + 1);
    for (Entry<String, ByteIterator> val : values.entrySet()) {
      attributes.put(val.getKey(), AttributeValue.fromS(val.getValue().toString()));
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
      rItems.put(attr.getKey(), new StringByteIterator(attr.getValue().s()));
    }
    return rItems;
  }

  private Map<String, AttributeValue> createPrimaryKey(String key) {
    Map<String, AttributeValue> k = new HashMap<>();
    if (primaryKeyType == PrimaryKeyType.HASH) {
      k.put(primaryKeyName, AttributeValue.fromS(key));
    } else if (primaryKeyType == PrimaryKeyType.HASH_AND_RANGE) {
      k.put(hashKeyName, AttributeValue.fromS(hashKeyValue));
      k.put(primaryKeyName, AttributeValue.fromS(key));
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
