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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import org.junit.*;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.scylladb.ScyllaDBContainer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Integration tests for the Scylla client
 */
public class ScyllaCQLClientTest {
  // Change the default Scylla timeout from 10s to 120s for slow CI machines
  private final static long timeout = 120000L;

  private final static String TABLE = "usertable";
  private final static String DEFAULT_ROW_KEY = "user1";
  private final static String KEYSPACE = "ycsb";

  private static String HOST;
  private static int PORT;

  private static String DATACENTER;

  private ScyllaCQLClient client;
  private CqlSession session;

  public static ScyllaDBContainer scyllaContainer;

  @BeforeClass
  public static void setUpContainer() {
    try {
      // Determine Scylla image from system properties, with sensible defaults
      // -Dscylla.image takes precedence. Otherwise built from -Dscylla.version (defaults to 2026.1)
      final String version = System.getProperty("scylla.version", "2026.1");
      final String imageProp = System.getProperty("scylla.image");
      final String image = (imageProp != null && !imageProp.isEmpty()) ? imageProp : ("scylladb/scylla:" + version);

      scyllaContainer = new ScyllaDBContainer(DockerImageName.parse(image));
      scyllaContainer.start();

      HOST = scyllaContainer.getHost();
      PORT = scyllaContainer.getMappedPort(9042);

      // Ensure keyspace exists
      try (CqlSession tmp = newSession(null)) {
        tmp.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
        DATACENTER = tmp.execute("SELECT data_center FROM system.local").one().getString("data_center");
      }
    } catch (Throwable t) {
      Assume.assumeTrue("Skipping Scylla tests because Docker/Testcontainers is not available: " + t.getMessage(), false);
    }
  }

  private static CqlSession newSession(String keyspace) {
    return CqlSession.builder()
        .addContactPoint(new InetSocketAddress(HOST, PORT))
        .withLocalDatacenter(DATACENTER == null ? "datacenter1" : DATACENTER)
        .withKeyspace(keyspace)
        .build();
  }

  private void createTableFromCqlFile() throws IOException {
    try (InputStream inputStream = getClass().getResourceAsStream("/ycsb.cql")) {
      if (inputStream == null) {
        throw new IOException("ycsb.cql file not found in resources");
      }

      String cqlContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

      // Replace the table name and key name, and make it CREATE TABLE IF NOT EXISTS
      String createTableStmt = cqlContent
          .replaceAll("usertable", TABLE)
          .replaceAll("y_id", ScyllaCQLClient.YCSB_KEY)
          .replaceAll("/\\*[\\s\\S]*?\\*/", "") // Remove comments
          .trim();

      session.execute(createTableStmt);
    }
  }

  @Before
  public void setUp() throws Exception {
    session = newSession(KEYSPACE);

    // Create table using the CQL file from resources
    createTableFromCqlFile();

    Properties p = new Properties();
    p.setProperty("scylla.hosts", HOST);
    p.setProperty("scylla.port", Integer.toString(PORT));
    p.setProperty("scylla.keyspace", KEYSPACE);
    p.setProperty("scylla.table", TABLE);

    Measurements.setProperties(p);

    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    client = new ScyllaCQLClient();
    client.setProperties(p);
    client.init();
  }

  @After
  public void tearDownClient() throws Exception {
    if (client != null) {
      client.cleanup();
    }
    client = null;
  }

  @After
  public void clearTable() {
    // Clear the table so that each test starts fresh.
    session.execute(QueryBuilder.truncate(TABLE).build());
    session.close();
    session = null;
  }

  @Test
  public void testReadMissingRow() {
    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = client.read(TABLE, "Missing row", null, result);
    assertThat(result.size(), is(0));
    assertThat(status, is(Status.NOT_FOUND));
  }

  private void insertRow() {
    session.execute(QueryBuilder.insertInto(TABLE)
        .value(ScyllaCQLClient.YCSB_KEY, QueryBuilder.literal(DEFAULT_ROW_KEY))
        .value("field0", QueryBuilder.literal("value1"))
        .value("field1", QueryBuilder.literal("value2"))
        .build());
  }

  @Test
  public void testRead() {
    insertRow();

    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, null, result);
    assertThat(status, is(Status.OK));
    assertThat(result.entrySet(), hasSize(11));
    assertThat(result, hasEntry("field2", null));

    final HashMap<String, String> strResult = new HashMap<>();
    for (final Map.Entry<String, ByteIterator> e : result.entrySet()) {
      if (e.getValue() != null) {
        strResult.put(e.getKey(), e.getValue().toString());
      }
    }
    assertThat(strResult, hasEntry(ScyllaCQLClient.YCSB_KEY, DEFAULT_ROW_KEY));
    assertThat(strResult, hasEntry("field0", "value1"));
    assertThat(strResult, hasEntry("field1", "value2"));
  }

  @Test
  public void testReadSingleColumn() {
    insertRow();
    final HashMap<String, ByteIterator> result = new HashMap<>();
    final Set<String> fields = Collections.singleton("field1");
    final Status status = client.read(TABLE, DEFAULT_ROW_KEY, fields, result);
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

    final Status status = client.insert(TABLE, key, StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    final SimpleStatement selectStmt =
        QueryBuilder.selectFrom(TABLE)
            .columns("field0", "field1")
            .whereColumn(ScyllaCQLClient.YCSB_KEY).isEqualTo(QueryBuilder.literal(key))
            .limit(1)
            .build();

    final ResultSet rs = session.execute(selectStmt);
    final Row row = rs.one();
    assertThat(row, notNullValue());
    assertThat(rs.one(), nullValue());
    assertThat(row.getString("field0"), is("value1"));
    assertThat(row.getString("field1"), is("value2"));
  }

  @Test
  public void testUpdate() {
    insertRow();
    final Map<String, String> input = new HashMap<>();
    input.put("field0", "new-value1");
    input.put("field1", "new-value2");

    final Status status = client.update(TABLE,
        DEFAULT_ROW_KEY,
        StringByteIterator.getByteIteratorMap(input));
    assertThat(status, is(Status.OK));

    // Verify result
    final SimpleStatement selectStmt =
        QueryBuilder.selectFrom(TABLE)
            .columns("field0", "field1")
            .whereColumn(ScyllaCQLClient.YCSB_KEY).isEqualTo(QueryBuilder.literal(DEFAULT_ROW_KEY))
            .limit(1)
            .build();

    final ResultSet rs = session.execute(selectStmt);
    final Row row = rs.one();
    assertThat(row, notNullValue());
    assertThat(rs.one(), nullValue());
    assertThat(row.getString("field0"), is("new-value1"));
    assertThat(row.getString("field1"), is("new-value2"));
  }

  @Test
  public void testDelete() {
    insertRow();

    final Status status = client.delete(TABLE, DEFAULT_ROW_KEY);
    assertThat(status, is(Status.OK));

    // Verify result
    final SimpleStatement selectStmt =
        QueryBuilder.selectFrom(TABLE)
            .columns("field0", "field1")
            .whereColumn(ScyllaCQLClient.YCSB_KEY).isEqualTo(QueryBuilder.literal(DEFAULT_ROW_KEY))
            .limit(1)
            .build();

    final ResultSet rs = session.execute(selectStmt);
    final Row row = rs.one();
    assertThat(row, nullValue());
  }

  @Test
  public void testScan() {
    insertRow();

    final Vector<HashMap<String, ByteIterator>> result = new Vector<>();
    final Status status = client.scan(TABLE, DEFAULT_ROW_KEY, 10, Collections.singleton("field0"), result);
    assertThat(status, is(Status.OK));
    assertThat(result, hasSize(1));
    assertThat(StringByteIterator.getStringMap(result.get(0)), hasEntry("field0", "value1"));
  }

  @Test
  public void testPreparedStatements() {
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

  @AfterClass
  public static void tearDownContainer() {
    if (scyllaContainer != null) {
      scyllaContainer.stop();
      scyllaContainer = null;
    }
  }
}
