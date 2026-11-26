/*
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
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
package site.ycsb.db.scylla;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

/**
 * Scylla DB implementation.
 */
public class ScyllaCQLClient extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScyllaCQLClient.class);

  private static CqlSession session = null;

  private static final ConcurrentMap<Set<String>, PreparedStatement> READ_STMTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> SCAN_STMTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> INSERT_STMTS = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Set<String>, PreparedStatement> UPDATE_STMTS = new ConcurrentHashMap<>();
  private static final AtomicReference<PreparedStatement> READ_ALL_STMT = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> SCAN_ALL_STMT = new AtomicReference<>();
  private static final AtomicReference<PreparedStatement> DELETE_STMT = new AtomicReference<>();

  private static ConsistencyLevel readConsistencyLevel = DefaultConsistencyLevel.QUORUM;
  private static ConsistencyLevel writeConsistencyLevel = DefaultConsistencyLevel.QUORUM;

  private static boolean lwt = false;

  public static final String YCSB_KEY = "y_id";
  public static final String KEYSPACE_PROPERTY = "scylla.keyspace";
  public static final String KEYSPACE_PROPERTY_DEFAULT = "ycsb";
  public static final String USERNAME_PROPERTY = "scylla.username";
  public static final String PASSWORD_PROPERTY = "scylla.password";

  public static final String HOSTS_PROPERTY = "scylla.hosts";
  public static final String PORT_PROPERTY = "scylla.port";
  public static final String PORT_PROPERTY_DEFAULT = "9042";

  public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "scylla.readconsistencylevel";
  public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = readConsistencyLevel.name();
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "scylla.writeconsistencylevel";
  public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = writeConsistencyLevel.name();

  public static final String MAX_CONNECTIONS_PROPERTY = "scylla.maxconnections";
  public static final String CORE_CONNECTIONS_PROPERTY = "scylla.coreconnections";
  public static final String CONNECT_TIMEOUT_MILLIS_PROPERTY = "scylla.connecttimeoutmillis";
  public static final String READ_TIMEOUT_MILLIS_PROPERTY = "scylla.readtimeoutmillis";

  public static final String SCYLLA_LWT = "scylla.lwt";

  public static final String TOKEN_AWARE_LOCAL_DC = "scylla.local_dc";

  public static final String TRACING_PROPERTY = "scylla.tracing";
  public static final String TRACING_PROPERTY_DEFAULT = "false";

  public static final String USE_SSL_CONNECTION = "scylla.useSSL";
  private static final String DEFAULT_USE_SSL_CONNECTION = "false";

  /**
   * Load balancing policy for sessions without {@link #TOKEN_AWARE_LOCAL_DC}.
   * It infers the local datacenter from the contact points.
   */
  private static final String DC_INFERRING_POLICY = "DcInferringLoadBalancingPolicy";

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static boolean debug = false;

  private static boolean trace = false;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {

    // Keep track of number of calls to init (for later cleanup)
    INIT_COUNT.incrementAndGet();

    // Synchronized so that we only have a single
    // session instance for all the threads.
    synchronized (INIT_COUNT) {

      // Check if the session has already been initialized
      if (session != null) {
        return;
      }

      try {

        debug = Boolean.parseBoolean(getProperties().getProperty("debug", "false"));
        trace = Boolean.parseBoolean(getProperties().getProperty(TRACING_PROPERTY, TRACING_PROPERTY_DEFAULT));

        String host = getProperties().getProperty(HOSTS_PROPERTY);
        if (host == null) {
          throw new DBException(String.format("Required property \"%s\" missing for scyllaCQLClient", HOSTS_PROPERTY));
        }
        String[] hosts = host.split(",");
        int port = Integer.parseInt(getProperties().getProperty(PORT_PROPERTY, PORT_PROPERTY_DEFAULT));

        String username = getProperties().getProperty(USERNAME_PROPERTY);
        String password = getProperties().getProperty(PASSWORD_PROPERTY);

        String keyspace = getProperties().getProperty(KEYSPACE_PROPERTY, KEYSPACE_PROPERTY_DEFAULT);

        readConsistencyLevel = DefaultConsistencyLevel.valueOf(
            getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY, READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));
        writeConsistencyLevel = DefaultConsistencyLevel.valueOf(
            getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY, WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT));

        boolean useSSL = Boolean.parseBoolean(
            getProperties().getProperty(USE_SSL_CONNECTION, DEFAULT_USE_SSL_CONNECTION));

        ProgrammaticDriverConfigLoaderBuilder config = DriverConfigLoader.programmaticBuilder();
        CqlSessionBuilder builder = CqlSession.builder().withKeyspace(keyspace);

        for (String contactPoint : hosts) {
          builder = builder.addContactPoint(new InetSocketAddress(contactPoint.trim(), port));
        }

        if ((username != null) && !username.isEmpty()) {
          builder = builder.withAuthCredentials(username, password);
          if (useSSL) {
            builder = builder.withSslContext(SSLContext.getDefault());
          }
        }

        final String localDC = getProperties().getProperty(TOKEN_AWARE_LOCAL_DC);
        if (localDC != null && !localDC.isEmpty()) {
          builder = builder.withLocalDatacenter(localDC);

          LOGGER.info("Using local datacenter with token awareness: {}\n", localDC);

          // If was not overridden explicitly, set LOCAL_QUORUM
          if (getProperties().getProperty(READ_CONSISTENCY_LEVEL_PROPERTY) == null) {
            readConsistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM;
          }

          if (getProperties().getProperty(WRITE_CONSISTENCY_LEVEL_PROPERTY) == null) {
            writeConsistencyLevel = DefaultConsistencyLevel.LOCAL_QUORUM;
          }
        } else {
          config = config.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DC_INFERRING_POLICY);
        }

        // The driver keeps a fixed number of connections per node.
        // The larger of the two connection properties sets that number.
        int connections = Math.max(
            Integer.parseInt(getProperties().getProperty(CORE_CONNECTIONS_PROPERTY, "0")),
            Integer.parseInt(getProperties().getProperty(MAX_CONNECTIONS_PROPERTY, "0")));
        if (connections > 0) {
          config = config.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, connections);
        }

        String connectTimeoutMillis = getProperties().getProperty(CONNECT_TIMEOUT_MILLIS_PROPERTY);
        if (connectTimeoutMillis != null) {
          config = config.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
              Duration.ofMillis(Long.parseLong(connectTimeoutMillis)));
        }

        String readTimeoutMillis = getProperties().getProperty(READ_TIMEOUT_MILLIS_PROPERTY);
        if (readTimeoutMillis != null) {
          config = config.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
              Duration.ofMillis(Long.parseLong(readTimeoutMillis)));
        }

        session = builder.withConfigLoader(config.build()).build();

        Metadata metadata = session.getMetadata();
        LOGGER.info("Connected to cluster: {}\n", metadata.getClusterName().orElse("unknown"));

        for (Node node : metadata.getNodes().values()) {
          LOGGER.info("Datacenter: {}; Host: {}; Rack: {}\n",
              node.getDatacenter(), node.getEndPoint().resolve(), node.getRack());
        }

        if (Boolean.parseBoolean(getProperties().getProperty(SCYLLA_LWT, Boolean.toString(lwt)))) {
          LOGGER.info("Using LWT\n");
          lwt = true;
          readConsistencyLevel = DefaultConsistencyLevel.SERIAL;
          writeConsistencyLevel = DefaultConsistencyLevel.ANY;
        } else {
          LOGGER.info("Not using LWT\n");
        }

        LOGGER.info("Read consistency: {}, Write consistency: {}\n",
            readConsistencyLevel.name(),
            writeConsistencyLevel.name());
      } catch (Exception e) {
        throw new DBException(e);
      }
    } // synchronized
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    synchronized (INIT_COUNT) {
      final int curInitCount = INIT_COUNT.decrementAndGet();
      if (curInitCount <= 0) {
        READ_STMTS.clear();
        SCAN_STMTS.clear();
        INSERT_STMTS.clear();
        UPDATE_STMTS.clear();
        READ_ALL_STMT.set(null);
        SCAN_ALL_STMT.set(null);
        DELETE_STMT.set(null);
        if (session != null) {
          session.close();
          session = null;
        }
      }
      if (curInitCount < 0) {
        // This should never happen.
        throw new DBException(String.format("initCount is negative: %d", curInitCount));
      }
    }
  }

  /**
   * Build a bound statement with the consistency level and tracing flag of this client.
   */
  private static BoundStatementBuilder bind(PreparedStatement stmt, ConsistencyLevel consistencyLevel,
                                            Object... values) {
    return stmt.boundStatementBuilder(values)
        .setConsistencyLevel(consistencyLevel)
        .setTracing(trace);
  }

  /**
   * Copy each column of a row into a field/value map.
   */
  private static void readRow(Row row, Map<String, ByteIterator> result) {
    ColumnDefinitions cd = row.getColumnDefinitions();
    for (int i = 0; i < cd.size(); i++) {
      String name = cd.get(i).getName().asInternal();
      ByteBuffer val = row.getBytesUnsafe(i);
      if (val != null) {
        byte[] bytes = new byte[val.remaining()];
        val.duplicate().get(bytes);
        result.put(name, new ByteArrayByteIterator(bytes));
      } else {
        result.put(name, null);
      }
    }
  }

  /**
   * Start a SELECT for the given fields, or for all columns when fields is null.
   */
  private static Select select(String table, Set<String> fields) {
    SelectFrom selectFrom = QueryBuilder.selectFrom(table);
    return (fields == null) ? selectFrom.all() : selectFrom.columns(fields);
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    try {
      PreparedStatement stmt = (fields == null) ? READ_ALL_STMT.get() : READ_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        stmt = session.prepare(select(table, fields)
            .whereColumn(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker())
            .limit(1)
            .build());

        PreparedStatement prevStmt = (fields == null) ?
            READ_ALL_STMT.getAndSet(stmt) :
            READ_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOGGER.debug(stmt.getQuery());
      LOGGER.debug("key = {}", key);

      ResultSet rs = session.execute(bind(stmt, readConsistencyLevel, key).build());

      // Should be only 1 row
      Row row = rs.one();
      if (row == null) {
        return Status.NOT_FOUND;
      }

      readRow(row, result);

      return Status.OK;

    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error reading key: {}", key).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * scylla CQL uses "token" method for range scan which doesn't always yield
   * intuitive results.
   *
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    try {
      PreparedStatement stmt = (fields == null) ? SCAN_ALL_STMT.get() : SCAN_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        stmt = session.prepare(select(table, fields)
            .whereToken(YCSB_KEY)
            .isGreaterThanOrEqualTo(QueryBuilder.raw("token(?)"))
            .limit(QueryBuilder.bindMarker())
            .build());

        PreparedStatement prevStmt = (fields == null) ?
            SCAN_ALL_STMT.getAndSet(stmt) :
            SCAN_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOGGER.debug(stmt.getQuery());
      LOGGER.debug("startKey = {}, recordcount = {}", startkey, recordcount);

      ResultSet rs = session.execute(bind(stmt, readConsistencyLevel, startkey, recordcount).build());

      for (Row row : rs) {
        HashMap<String, ByteIterator> tuple = new HashMap<>();
        readRow(row, tuple);
        result.add(tuple);
      }

      return Status.OK;

    } catch (Exception e) {
      LOGGER.error(
          MessageFormatter.format("Error scanning with startkey: {}", startkey).getMessage(), e);
      return Status.ERROR;
    }

  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = UPDATE_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        List<Assignment> assignments = new ArrayList<>(fields.size());
        for (String field : fields) {
          assignments.add(Assignment.setColumn(field, QueryBuilder.bindMarker()));
        }

        Update updateStmt = QueryBuilder.update(table)
            .set(assignments)
            .whereColumn(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker());

        if (lwt) {
          updateStmt = updateStmt.ifExists();
        }

        stmt = session.prepare(updateStmt.build());

        PreparedStatement prevStmt = UPDATE_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(stmt.getQuery());
        LOGGER.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOGGER.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add fields
      ColumnDefinitions vars = stmt.getVariableDefinitions();
      BoundStatementBuilder boundStmt = bind(stmt, writeConsistencyLevel);
      for (int i = 0; i < vars.size() - 1; i++) {
        boundStmt = boundStmt.setString(i, values.get(vars.get(i).getName().asInternal()).toString());
      }

      // Add key
      boundStmt = boundStmt.setString(vars.size() - 1, key);

      session.execute(boundStmt.build());

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = INSERT_STMTS.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        // Add key
        RegularInsert regularInsert = QueryBuilder.insertInto(table)
            .value(YCSB_KEY, QueryBuilder.bindMarker());

        // Add fields
        for (String field : fields) {
          regularInsert = regularInsert.value(field, QueryBuilder.bindMarker());
        }

        Insert insertStmt = lwt ? regularInsert.ifNotExists() : regularInsert;

        stmt = session.prepare(insertStmt.build());

        PreparedStatement prevStmt = INSERT_STMTS.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(stmt.getQuery());
        LOGGER.debug("key = {}", key);
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
          LOGGER.debug("{} = {}", entry.getKey(), entry.getValue());
        }
      }

      // Add key
      BoundStatementBuilder boundStmt = bind(stmt, writeConsistencyLevel).setString(0, key);

      // Add fields
      ColumnDefinitions vars = stmt.getVariableDefinitions();
      for (int i = 1; i < vars.size(); i++) {
        boundStmt = boundStmt.setString(i, values.get(vars.get(i).getName().asInternal()).toString());
      }

      session.execute(boundStmt.build());

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Delete a record from the database.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {

    try {
      PreparedStatement stmt = DELETE_STMT.get();

      // Prepare statement on demand
      if (stmt == null) {
        Delete deleteStmt = QueryBuilder.deleteFrom(table)
            .whereColumn(YCSB_KEY).isEqualTo(QueryBuilder.bindMarker());

        if (lwt) {
          deleteStmt = deleteStmt.ifExists();
        }

        stmt = session.prepare(deleteStmt.build());

        PreparedStatement prevStmt = DELETE_STMT.getAndSet(stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      LOGGER.debug(stmt.getQuery());
      LOGGER.debug("key = {}", key);

      session.execute(bind(stmt, writeConsistencyLevel, key).build());

      return Status.OK;
    } catch (Exception e) {
      LOGGER.error(MessageFormatter.format("Error deleting key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

}
