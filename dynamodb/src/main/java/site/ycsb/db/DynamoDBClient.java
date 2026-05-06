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

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * DynamoDB binding for YCSB.
 *
 * <p>Selects between two backing implementations via {@code dynamodb.sdkVersion}:
 * <ul>
 *   <li>{@code v2} (default) — AWS SDK v2 async client over Netty NIO.
 *       See {@link DynamoDBV2Operations}.</li>
 *   <li>{@code v1} — legacy AWS SDK v1 sync client over Apache HTTP. Restored from the
 *       pre-v1.1.0 binding for back-to-back perf comparisons. Does not support
 *       {@code dynamodb.alternator.loadbalancing}. See {@link DynamoDBV1Operations}.</li>
 * </ul>
 *
 * <p>Both paths share the same property surface (primary keys, credentials, TTL, etc.) plus
 * a small set of pool-tuning knobs: {@code dynamodb.connectMax},
 * {@code dynamodb.connectionTimeToLiveSeconds}, {@code dynamodb.connectionMaxIdleTimeSeconds},
 * {@code dynamodb.maxPendingConnectionAcquires} (v2 only).
 */
public final class DynamoDBClient extends DB {

  static {
    // Disable the JVM positive DNS cache before any DNS lookup happens, so each new HTTP
    // connection re-resolves the hostname and picks up the next IP from the OS resolver's
    // round-robin rotation. Setting this in a static block on the binding class ensures it
    // runs before sun.net.InetAddressCachePolicy is initialised.
    java.security.Security.setProperty("networkaddress.cache.ttl", "0");
    java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
  }

  private DynamoDBOperations operations;

  @Override
  public void init() throws DBException {
    var props = getProperties();
    var sdkVersion = props.getProperty("dynamodb.sdkVersion", "v2").trim().toLowerCase();
    operations = switch (sdkVersion) {
      case "v1" -> new DynamoDBV1Operations();
      case "v2" -> new DynamoDBV2Operations();
      default -> throw new DBException(
          "Invalid dynamodb.sdkVersion=" + sdkVersion + " (expected 'v1' or 'v2')");
    };
    operations.init(props);
  }

  @Override
  public void cleanup() throws DBException {
    if (operations != null) {
      operations.cleanup();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return operations.read(table, key, fields, result);
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return operations.scan(table, startkey, recordcount, fields, result);
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return operations.update(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return operations.insert(table, key, values);
  }

  @Override
  public Status delete(String table, String key) {
    return operations.delete(table, key);
  }
}
