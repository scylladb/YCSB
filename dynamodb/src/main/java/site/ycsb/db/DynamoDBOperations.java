/*
 * Copyright (c) 2026 YCSB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Internal SDK-agnostic operations facade. {@link DynamoDBClient} dispatches every YCSB
 * operation to one of these implementations based on {@code dynamodb.sdkVersion}.
 */
interface DynamoDBOperations {
  void init(Properties props) throws DBException;

  void cleanup() throws DBException;

  Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result);

  Status scan(String table, String startkey, int recordcount, Set<String> fields,
              Vector<HashMap<String, ByteIterator>> result);

  Status update(String table, String key, Map<String, ByteIterator> values);

  Status insert(String table, String key, Map<String, ByteIterator> values);

  Status delete(String table, String key);
}
