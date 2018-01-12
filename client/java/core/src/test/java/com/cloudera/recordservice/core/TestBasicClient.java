// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.core;

import static org.junit.Assume.assumeFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

// TODO: add more stats tests.
// TODO: add testes to verify that we don't retry when the error is not retryable.
public class TestBasicClient extends TestBase {

  // If true, RecordService is being run on the quickstart vm. There are a few
  // tests that we need to skip in this mode.
  public static final boolean RECORD_SERVICE_QUICKSTART_VM =
      System.getenv("RECORD_SERVICE_QUICKSTART_VM") != null &&
      System.getenv("RECORD_SERVICE_QUICKSTART_VM").equalsIgnoreCase("true");

  // The number of iterations to run the multithreaded test.
  static private final int MULTITHREADED_TEST_NUM_ITERATIONS = 20;
  static private final int MULTITHREADED_TEST_NUM_THREADS = 5;

  void fetchAndVerifyCount(Records records, int expectedCount)
      throws RecordServiceException, IOException {
    int count = 0;
    while (records.hasNext()) {
      ++count;
      records.next();
    }
    assertEquals(expectedCount, count);
  }

  @Test
  public void testPlannerConnection()
      throws RuntimeException, IOException, RecordServiceException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);
    // Test calling the APIs after close.
    planner.close();
    boolean exceptionThrown = false;
    try {
      planner.getProtocolVersion();
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Client not connected."));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      planner.planRequest(Request.createSqlRequest("ABCD"));
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Client not connected."));
    } finally {
      assertTrue(exceptionThrown);
    }

    planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);

    // Call it twice and make sure it's fine.
    assertEquals(planner.getProtocolVersion(), planner.getProtocolVersion());

    // Plan a request.
    planner.planRequest(Request.createSqlRequest(
        String.format("select * from %s", DEFAULT_TEST_TABLE)));

    // Closing repeatedly is fine.
    planner.close();
    planner.close();

    // Try connecting to a bad planner.
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().connect(PLANNER_HOST, 12345);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServicePlanner"));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().planRequest("Bad", 1234, null);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServicePlanner"));
    } finally {
      assertTrue(exceptionThrown);
    }

    // Try a bad port that is another thrift service.
    exceptionThrown = false;
    RecordServicePlannerClient client = null;
    try {
      client = new RecordServicePlannerClient.Builder().
          setMaxAttempts(1).connect(PLANNER_HOST, DEFAULT_WORKER_PORT);
      client.getSchema(Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getCause().getMessage().contains(
          "Invalid method name"));
    } finally {
      if (client != null) client.close();
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testWorkerConnection()
      throws RuntimeException, IOException, RecordServiceException {
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder()
            .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);

    // Call it twice and make sure it's fine.
    assertEquals(worker.getProtocolVersion(), worker.getProtocolVersion());

    assertEquals(0, worker.numActiveTasks());
    worker.close();

    // Close again.
    worker.close();
  }

  /**
   * Verify the server protocol version is within the list of the client lib.
   * TODO: add invalid tests when we have new server with unsupported protocol version
   */
  @Test
  public void testProtocolVersion() throws RecordServiceException, IOException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);
    ProtocolVersion serverVersion;
    try {
      serverVersion = planner.getProtocolVersion();
      assertTrue("Current RecordServiceClient does not support server protocol version: "
          + serverVersion.getVersion(), serverVersion.isValidProtocolVersion());
    } finally {
      planner.close();
    }

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);
    try {
      serverVersion = worker.getProtocolVersion();
      assertTrue("Current RecordServiceClient does not support server protocol version: "
          + serverVersion.getVersion(), serverVersion.isValidProtocolVersion());
    } finally {
      worker.close();
    }
  }

  @Test
  public void testConnectionDrop() throws IOException, RecordServiceException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .setMaxAttempts(1).setSleepDurationMs(0)
        .connect(PLANNER_HOST, PLANNER_PORT);
    PlanRequestResult plan =
        planner.planRequest(Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    Task task = plan.tasks.get(0);

    // Simulate dropping the connection.
    planner.closeConnectionForTesting();

    // Try using the planner connection.
    boolean exceptionThrown = false;
    try {
      planner.planRequest(Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    // Simulate dropping the connection.
    planner.closeConnectionForTesting();
    try {
      planner.getSchema(Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Close should still do something reasonable.
    planner.close();

    // Testing failures, don't retry.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setMaxAttempts(1).setSleepDurationMs(0)
        .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);

    fetchAndVerifyCount(worker.execAndFetch(task), 25);

    // Try this again.
    fetchAndVerifyCount(worker.execAndFetch(task), 25);

    // Simulate dropping the worker connection
    worker.closeConnectionForTesting();

    // Try executing a task.
    exceptionThrown = false;
    try {
      worker.execAndFetch(task);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      worker.execTask(task);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Reestablish connection.
    worker.close();
    worker = new RecordServiceWorkerClient.Builder()
        .setMaxAttempts(1).setSleepDurationMs(0)
        .setFetchSize(1)
        .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);

    // Execute a fetch once.
    RecordServiceWorkerClient.TaskState handle = worker.execTask(task);
    worker.fetch(handle);
    worker.getTaskStatus(handle);

    // Drop the connection.
    worker.closeConnectionForTesting();

    // Try to fetch more.
    // TODO: what does retry, fault tolerance here look like?
    exceptionThrown = false;
    try {
      worker.fetch(handle);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Try to get stats
    exceptionThrown = false;
    try {
      worker.getTaskStatus(handle);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not reach service."));
    }
    assertTrue(exceptionThrown);

    // Try to close the task. The connection is bad so this won't close the
    // task on the server side.
    worker.closeTask(handle);

    // Closing the worker should behave reasonably.
    worker.close();
  }

  // Tests that the delegation token APIs fail gracefully if called to a
  // non-secure server.
  @Test
  public void testUnsecureConnectionTokens() throws IOException,
      RecordServiceException, InterruptedException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);
    boolean exceptionThrown = false;
    try {
      planner.getDelegationToken(null);
    } catch (RecordServiceException e) {
      assertTrue(e.code == RecordServiceException.ErrorCode.AUTHENTICATION_ERROR);
      assertTrue(e.getMessage().contains(
          "can only be called with a Kerberos connection."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    DelegationToken dummyToken = new DelegationToken("a", "b", new byte[1]);

    exceptionThrown = false;
    try {
      planner.cancelDelegationToken(dummyToken);
    } catch (RecordServiceException e) {
      assertTrue(e.code == RecordServiceException.ErrorCode.AUTHENTICATION_ERROR);
      assertTrue(e.getMessage().contains(
          "can only be called from a secure connection."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      planner.renewDelegationToken(dummyToken);
    } catch (RecordServiceException e) {
      assertTrue(e.code == RecordServiceException.ErrorCode.AUTHENTICATION_ERROR);
      assertTrue(e.getMessage().contains(
          "can only be called with a Kerberos connection."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
    planner.close();
  }

  @Test
  public void testPlannerTimeout() throws IOException, RecordServiceException {
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setMaxAttempts(1).setSleepDurationMs(0)
          .setRpcTimeoutMs(1)
          .planRequest(PLANNER_HOST, PLANNER_PORT,
              Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setMaxAttempts(1).setSleepDurationMs(0).setRpcTimeoutMs(1)
          .getSchema(PLANNER_HOST, PLANNER_PORT,
              Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testWorkerTimeout() throws IOException, RecordServiceException {
    Task task = new RecordServicePlannerClient.Builder()
        .setMaxAttempts(1).setSleepDurationMs(0)
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest(DEFAULT_TEST_TABLE)).tasks.get(0);

    boolean exceptionThrown = false;

    try {
      RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
          .setMaxAttempts(1).setSleepDurationMs(0).setRpcTimeoutMs(1)
          .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);
      worker.execAndFetch(task);
      worker.close();
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testWorkerMisuse() throws IOException, RecordServiceException {
    // Connect to a non-existent service
    boolean exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder().connect("bad", PLANNER_PORT);
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Could not connect to RecordServiceWorker"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testTaskClose() throws RecordServiceException, IOException {
    // Plan the request
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(
            String.format("select * from %s", DEFAULT_TEST_TABLE)));

    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect(addr.hostname, addr.port);
    assertEquals(0, worker.numActiveTasks());

    worker.execTask(plan.tasks.get(0));
    assertEquals(1, worker.numActiveTasks());
    worker.execTask(plan.tasks.get(0));
    RecordServiceWorkerClient.TaskState handle = worker.execTask(plan.tasks.get(0));
    assertEquals(3, worker.numActiveTasks());
    worker.closeTask(handle);
    assertEquals(2, worker.numActiveTasks());

    // Try to get task status with a closed handle.
    boolean exceptionThrown = false;
    try {
      worker.getTaskStatus(handle);
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Invalid task handle."));
    }
    assertTrue(exceptionThrown);

    // Close again. Should be fine.
    worker.closeTask(handle);
    assertEquals(2, worker.numActiveTasks());

    // Closing the worker should close them all.
    worker.close();
    assertEquals(0, worker.numActiveTasks());
  }

  @Test
  public void testNation() throws RecordServiceException, IOException {
    // Plan the request
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(
            String.format("select * from %s", DEFAULT_TEST_TABLE)));
    assertTrue(plan.warnings.isEmpty());
    // Verify schema
    verifyNationSchema(plan.schema, false);
    assertEquals(1, plan.tasks.size());
    assertTrue(plan.tasks.get(0).localHosts.size() >= 1);

    // Execute the task
    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect(addr.hostname, addr.port);

    // Serialize/deserialize the task to test the Task code.
    Task originalTask = plan.tasks.get(0);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    originalTask.serialize(new DataOutputStream(outputStream));
    outputStream.flush();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(outputStream.toByteArray());
    Task task = Task.deserialize(new DataInputStream(inputStream));

    Records records = worker.execAndFetch(task);
    int numRecords = 0;
    while (records.hasNext()) {
      Records.Record record = records.next();
      ++numRecords;
      if (numRecords == 1) {
        assertFalse(record.isNull(0));
        assertFalse(record.isNull(1));
        assertFalse(record.isNull(2));
        assertFalse(record.isNull(3));

        assertEquals(0, record.nextShort(0));
        assertEquals("ALGERIA", record.nextByteArray(1).toString());
        assertEquals(0, record.nextShort(2));
        assertEquals(" haggle. carefully final deposits detect slyly agai",
            record.nextByteArray(3).toString());
      }
    }

    // Reading off the end should fail gracefully.
    boolean exceptionThrown = false;
    try {
      records.next();
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("End of stream"));
    }
    assertTrue(exceptionThrown);

    // Verify status
    TaskStatus status = records.getStatus();
    assertTrue(status.dataErrors.isEmpty());
    assertTrue(status.warnings.isEmpty());

    TaskStatus.Stats stats = status.stats;
    assertEquals(1, stats.taskProgress, 0.01);
    assertEquals(25, stats.numRecordsRead);
    assertEquals(25, stats.numRecordsReturned);
    assertEquals(1, records.progress(), 0.01);

    records.close();

    assertEquals(25, numRecords);

    // Close and run this again. The worker object should still work.
    assertEquals(0, worker.numActiveTasks());
    worker.close();

    assertEquals(0, worker.numActiveTasks());
    worker.close();
  }

  @Test
  public void testNationWithUtility() throws RecordServiceException, IOException {
    // Plan the request
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(
            String.format("select * from %s", DEFAULT_TEST_TABLE)));
    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRecords = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRecords;
        if (numRecords == 1) {
          assertEquals(0, record.nextShort(0));
          assertEquals("ALGERIA", record.nextByteArray(1).toString());
          assertEquals(0, record.nextShort(2));
          assertEquals(" haggle. carefully final deposits detect slyly agai",
              record.nextByteArray(3).toString());
        }
      }
      records.close();
      assertEquals(25, numRecords);

      // Closing records again is idempotent
      records.close();

      // Try using records object after close.
      boolean exceptionThrown = false;
      try {
        records.getStatus();
      } catch (RuntimeException e) {
        exceptionThrown = true;
        assertTrue(e.getMessage(), e.getMessage().contains("Task already closed."));
      }
      assertTrue(exceptionThrown);
    }
  }

  /*
   * Verifies that the schema matches the alltypes table schema.
   */
  private void verifyAllTypesSchema(Schema inputSchema) throws IOException {
    // Serialize/deserialize the schema before verifying to test that code.
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    inputSchema.serialize(new DataOutputStream(outputStream));
    outputStream.flush();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(outputStream.toByteArray());
    Schema schema = Schema.deserialize(new DataInputStream(inputStream));

    assertEquals(12, schema.cols.size());
    assertEquals("bool_col", schema.cols.get(0).name);
    assertEquals(Schema.Type.BOOLEAN, schema.cols.get(0).type.typeId);
    assertEquals("tinyint_col", schema.cols.get(1).name);
    assertEquals(Schema.Type.TINYINT, schema.cols.get(1).type.typeId);
    assertEquals("smallint_col", schema.cols.get(2).name);
    assertEquals(Schema.Type.SMALLINT, schema.cols.get(2).type.typeId);
    assertEquals("int_col", schema.cols.get(3).name);
    assertEquals(Schema.Type.INT, schema.cols.get(3).type.typeId);
    assertEquals("bigint_col", schema.cols.get(4).name);
    assertEquals(Schema.Type.BIGINT, schema.cols.get(4).type.typeId);
    assertEquals("float_col", schema.cols.get(5).name);
    assertEquals(Schema.Type.FLOAT, schema.cols.get(5).type.typeId);
    assertEquals("double_col", schema.cols.get(6).name);
    assertEquals(Schema.Type.DOUBLE, schema.cols.get(6).type.typeId);
    assertEquals("string_col", schema.cols.get(7).name);
    assertEquals(Schema.Type.STRING, schema.cols.get(7).type.typeId);
    assertEquals("varchar_col", schema.cols.get(8).name);
    assertEquals(Schema.Type.VARCHAR, schema.cols.get(8).type.typeId);
    assertEquals(10, schema.cols.get(8).type.len);
    assertEquals("char_col", schema.cols.get(9).name);
    assertEquals(Schema.Type.CHAR, schema.cols.get(9).type.typeId);
    assertEquals(5, schema.cols.get(9).type.len);
    assertEquals("timestamp_col", schema.cols.get(10).name);
    assertEquals(Schema.Type.TIMESTAMP_NANOS, schema.cols.get(10).type.typeId);
    assertEquals("decimal_col", schema.cols.get(11).name);
    assertEquals(Schema.Type.DECIMAL, schema.cols.get(11).type.typeId);
    assertEquals(24, schema.cols.get(11).type.precision);
    assertEquals(10, schema.cols.get(11).type.scale);
  }

  private void verifyAllTypes(PlanRequestResult plan)
      throws RecordServiceException, IOException {
    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect(addr.hostname, addr.port);

    verifyAllTypesSchema(plan.schema);

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));

    // Execute the task
    assertEquals(2, plan.tasks.size());
    for (int t = 0; t < 2; ++t) {
      assertTrue(plan.tasks.get(t).localHosts.size() >= 1);
      Records records = worker.execAndFetch(plan.tasks.get(t));
      verifyAllTypesSchema(records.getSchema());
      assertTrue(records.hasNext());
      Records.Record record = records.next();

      if (record.nextBoolean(0)) {
        assertEquals(0, record.nextByte(1));
        assertEquals(1, record.nextShort(2));
        assertEquals(2, record.nextInt(3));
        assertEquals(3, record.nextLong(4));
        assertEquals(4.0, record.nextFloat(5), 0.1);
        assertEquals(5.0, record.nextDouble(6), 0.1);
        assertEquals("hello", record.nextByteArray(7).toString());
        assertEquals("vchar1", record.nextByteArray(8).toString());
        assertEquals("char1", record.nextByteArray(9).toString());
        assertEquals("2015-01-01",
            format.format(record.nextTimestampNanos(10).toTimeStamp()));
        assertEquals(new BigDecimal("3.1415920000"),
            record.nextDecimal(11).toBigDecimal());
      } else {
        assertEquals(6, record.nextByte(1));
        assertEquals(7, record.nextShort(2));
        assertEquals(8, record.nextInt(3));
        assertEquals(9, record.nextLong(4));
        assertEquals(10.0, record.nextFloat(5), 0.1);
        assertEquals(11.0, record.nextDouble(6), 0.1);
        assertEquals("world", record.nextByteArray(7).toString());
        assertEquals("vchar2", record.nextByteArray(8).toString());
        assertEquals("char2", record.nextByteArray(9).toString());
        assertEquals("2016-01-01",
            format.format(record.nextTimestampNanos(10).toTimeStamp()));
        assertEquals(new BigDecimal("1234.5678900000"),
            record.nextDecimal(11).toBigDecimal());
      }

      assertFalse(records.hasNext());
      records.close();
    }

    assertEquals(0, worker.numActiveTasks());
    worker.close();
  }

  /*
   * Verifies that the schema matches the nation table schema.
   */
  private void verifyNationSchema(Schema schema, boolean parquet) {
    assertEquals(4, schema.cols.size());
    assertEquals("n_nationkey", schema.cols.get(0).name);
    assertEquals(parquet ? Schema.Type.INT : Schema.Type.SMALLINT,
        schema.cols.get(0).type.typeId);
    assertEquals("n_name", schema.cols.get(1).name);
    assertEquals(Schema.Type.STRING, schema.cols.get(1).type.typeId);
    assertEquals("n_regionkey", schema.cols.get(2).name);
    assertEquals(parquet ? Schema.Type.INT : Schema.Type.SMALLINT,
        schema.cols.get(2).type.typeId);
    assertEquals("n_comment", schema.cols.get(3).name);
    assertEquals(Schema.Type.STRING, schema.cols.get(3).type.typeId);
  }

  @Test
  public void testAllTypes() throws RecordServiceException, IOException {
    // Just ask for the schema.
    GetSchemaResult schemaResult = new RecordServicePlannerClient.Builder()
        .getSchema(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest("rs.alltypes"));

    verifyAllTypesSchema(schemaResult.schema);

    // Plan the request
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from rs.alltypes"));
    verifyAllTypes(plan);
  }

  @Test
  public void testAllTypesEmpty() throws RecordServiceException, IOException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from rs.alltypes_empty"));
    assertEquals(0, plan.tasks.size());
    verifyAllTypesSchema(plan.schema);
  }

  // Returns all the strings from running plan as a list. The plan must
  // have a schema that returns a single string column.
  List<String> getAllStrings(PlanRequestResult plan)
      throws RecordServiceException, IOException {
    List<String> results = Lists.newArrayList();
    assertEquals(1, plan.schema.cols.size());
    assertEquals(Schema.Type.STRING, plan.schema.cols.get(0).type.typeId);
    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = null;
      try {
        records = WorkerClientUtil.execTask(plan, i);
        while (records.hasNext()) {
          Records.Record record = records.next();
          results.add(record.nextByteArray(0).toString());
        }
      } finally {
        if (records != null) records.close();
      }
    }
    return results;
  }

  @Test
  public void testNationPath() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch.nation/"));
    assertEquals(1, plan.tasks.size());
    List<String> lines = getAllStrings(plan);
    assertEquals(25, lines.size());
    assertEquals("6|FRANCE|3|refully final requests. regular, ironi", lines.get(6));
  }

  @Test
  public void testNationPathWithSchema() throws IOException, RecordServiceException {
    // The normal schema is SMALLINT, STRING, SMALLINT, STRING

    // Test with an all string schema.
    Schema schema = new Schema();
    schema.cols.add(
        new Schema.ColumnDesc("col1", new Schema.TypeDesc(Schema.Type.STRING)));
    schema.cols.add(
        new Schema.ColumnDesc("col2", new Schema.TypeDesc(Schema.Type.STRING)));
    schema.cols.add(
        new Schema.ColumnDesc("col3", new Schema.TypeDesc(Schema.Type.STRING)));
    schema.cols.add(
        new Schema.ColumnDesc("col4", new Schema.TypeDesc(Schema.Type.STRING)));
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch.nation/")
                .setSchema(schema).setFieldDelimiter('|'));
    assertEquals(1, plan.tasks.size());

    // Verify schema.
    assertEquals(schema.cols.size(), plan.schema.cols.size());
    for (int i = 0; i < plan.schema.cols.size(); ++i) {
      assertEquals(schema.cols.get(i).name, plan.schema.cols.get(i).name);
      assertEquals(schema.cols.get(i).type.typeId, plan.schema.cols.get(i).type.typeId);
    }

    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);
    RecordServiceWorkerClient worker =
        new RecordServiceWorkerClient.Builder().connect(addr.hostname, addr.port);

    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      Records.Record record = records.next();
      ++numRecords;
      if (numRecords == 1) {
        assertEquals("0", record.nextByteArray(0).toString());
        assertEquals("ALGERIA", record.nextByteArray(1).toString());
        assertEquals("0", record.nextByteArray(2).toString());
        assertEquals(" haggle. carefully final deposits detect slyly agai",
            record.nextByteArray(3).toString());
      }
    }
    assertEquals(25, numRecords);
    records.close();

    // Remap string cols to smallint, these should return null.
    schema.cols.set(1,
        new Schema.ColumnDesc("col2", new Schema.TypeDesc(Schema.Type.SMALLINT)));
    schema.cols.set(3,
        new Schema.ColumnDesc("col4", new Schema.TypeDesc(Schema.Type.SMALLINT)));

    plan = new RecordServicePlannerClient.Builder().planRequest(PLANNER_HOST, PLANNER_PORT,
        Request.createPathRequest("/test-warehouse/tpch.nation/")
            .setSchema(schema).setFieldDelimiter('|'));
    assertEquals(1, plan.tasks.size());

    // Verify schema.
    assertEquals(schema.cols.size(), plan.schema.cols.size());
    for (int i = 0; i < plan.schema.cols.size(); ++i) {
      assertEquals(schema.cols.get(i).name, plan.schema.cols.get(i).name);
      assertEquals(schema.cols.get(i).type.typeId, plan.schema.cols.get(i).type.typeId);
    }

    records = worker.execAndFetch(plan.tasks.get(0));
    numRecords = 0;
    while (records.hasNext()) {
      Records.Record record = records.next();
      ++numRecords;
      assertTrue(record.isNull(1));
      assertTrue(record.isNull(3));
      if (numRecords == 1) {
        assertEquals("0", record.nextByteArray(0).toString());
        assertEquals("0", record.nextByteArray(2).toString());
      }
    }
    assertEquals(25, numRecords);
    records.close();

    worker.close();
  }

  @Test
  public void testAllTypesPathWithSchema() throws IOException, RecordServiceException {
    assumeFalse(RECORD_SERVICE_QUICKSTART_VM);

    // Create the exact all types schema.
    Schema schema = new Schema();
    schema.cols.add(
        new Schema.ColumnDesc("bool_col", new Schema.TypeDesc(Schema.Type.BOOLEAN)));
    schema.cols.add(
        new Schema.ColumnDesc("tinyint_col", new Schema.TypeDesc(Schema.Type.TINYINT)));
    schema.cols.add(
        new Schema.ColumnDesc("smallint_col", new Schema.TypeDesc(Schema.Type.SMALLINT)));
    schema.cols.add(
        new Schema.ColumnDesc("int_col", new Schema.TypeDesc(Schema.Type.INT)));
    schema.cols.add(
        new Schema.ColumnDesc("bigint_col", new Schema.TypeDesc(Schema.Type.BIGINT)));
    schema.cols.add(
        new Schema.ColumnDesc("float_col", new Schema.TypeDesc(Schema.Type.FLOAT)));
    schema.cols.add(
        new Schema.ColumnDesc("double_col", new Schema.TypeDesc(Schema.Type.DOUBLE)));
    schema.cols.add(
        new Schema.ColumnDesc("string_col", new Schema.TypeDesc(Schema.Type.STRING)));
    schema.cols.add(
        new Schema.ColumnDesc("varchar_col", Schema.TypeDesc.createVarCharType(10)));
    schema.cols.add(
        new Schema.ColumnDesc("char_col", Schema.TypeDesc.createCharType(5)));
    schema.cols.add(
        new Schema.ColumnDesc("timestamp_col", new Schema.TypeDesc(Schema.Type.TIMESTAMP_NANOS)));
    schema.cols.add(
        new Schema.ColumnDesc("decimal_col", Schema.TypeDesc.createDecimalType(24, 10)));

    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
              Request.createPathRequest("/test-warehouse/rs.db/alltypes")
                  .setSchema(schema));
    verifyAllTypes(plan);
  }

  @Test
  public void testNationPathParquet() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch_nation_parquet/nation.parq"));
    assertEquals(1, plan.tasks.size());
    verifyNationSchema(plan.schema, true);
    fetchAndVerifyCount(WorkerClientUtil.execTask(plan, 0), 25);
  }

  @Test
  public void testPathWithLowTimeout() throws RecordServiceException{
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setRpcTimeoutMs(2).setMaxAttempts(1)
          .planRequest(PLANNER_HOST, PLANNER_PORT,
              Request.createPathRequest("/test-warehouse/tpch.nation/"));
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setRpcTimeoutMs(2).setMaxAttempts(1)
          .planRequest(PLANNER_HOST, PLANNER_PORT,
              Request.createPathRequest(
                  "/test-warehouse/tpch_nation_parquet/nation.parq"));
    } catch (IOException e) {
      assertTrue(e.getCause().getMessage().contains("java.net.SocketTimeoutException"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  void testNationPathGlobbing(String path, boolean expectMatch)
      throws IOException, RecordServiceException {
    try {
      PlanRequestResult plan = new RecordServicePlannerClient.Builder()
          .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createPathRequest(path));
      assertEquals(expectMatch ? 1 : 0, plan.tasks.size());
    } catch (RecordServiceException e) {
      assertFalse(expectMatch);
      assertTrue(e.code == RecordServiceException.ErrorCode.INVALID_REQUEST);
    }
  }

  @Test
  public void testNationPathGlobbing() throws IOException, RecordServiceException {
    // Non-matches
    testNationPathGlobbing("/test-warehouse/tpch.nation/*t", false);
    testNationPathGlobbing("/test-warehouse/tpch.nation/tbl", false);
    testNationPathGlobbing("/test-warehouse/tpch.nation*", false);
    // TODO: this should work.
    testNationPathGlobbing("/test-warehouse/tpch.*/*", false);

    // No trailing slash is okay
    testNationPathGlobbing("/test-warehouse/tpch.nation", true);
    // Trailing slashes is okay
    testNationPathGlobbing("/test-warehouse/tpch.nation/", true);
    // Multiple trailing slashes is okay
    testNationPathGlobbing("/test-warehouse/tpch.nation///", true);

    // Match for *
    testNationPathGlobbing("/test-warehouse/tpch.nation/*", true);
    testNationPathGlobbing("/test-warehouse/tpch.nation/*.tbl", true);
    testNationPathGlobbing("/test-warehouse/tpch.nation/*.tb*", true);
  }

  @Test
  public void testNationPathFiltering() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createPathRequest("/test-warehouse/tpch.nation/")
                .setQuery("select * from __PATH__ where record like '6|FRANCE%'"));
    assertEquals(1, plan.tasks.size());
    List<String> lines = getAllStrings(plan);
    assertEquals(1, lines.size());
    assertEquals("6|FRANCE|3|refully final requests. regular, ironi", lines.get(0));
  }

  @Test
  public void testNationView() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest("rs.nation_projection"));
    assertEquals(1, plan.tasks.size());
    assertEquals(2, plan.schema.cols.size());
    assertEquals("n_nationkey", plan.schema.cols.get(0).name);
    assertEquals("n_name", plan.schema.cols.get(1).name);

    for (int i = 0; i < plan.tasks.size(); ++i) {
      Records records = WorkerClientUtil.execTask(plan, i);
      int numRecords = 0;
      while (records.hasNext()) {
        Records.Record record = records.next();
        ++numRecords;
        switch (numRecords) {
        case 1:
          assertEquals(0, record.nextShort(0));
          assertEquals("ALGERIA", record.nextByteArray(1).toString());
          break;
        case 2:
          assertEquals(1, record.nextShort(0));
          assertEquals("ARGENTINA", record.nextByteArray(1).toString());
          break;
        case 3:
          assertEquals(2, record.nextShort(0));
          assertEquals("BRAZIL", record.nextByteArray(1).toString());
          break;
        case 4:
          assertEquals(3, record.nextShort(0));
          assertEquals("CANADA", record.nextByteArray(1).toString());
          break;
        case 5:
          assertEquals(4, record.nextShort(0));
          assertEquals("EGYPT", record.nextByteArray(1).toString());
          break;
        }
      }
      records.close();
      assertEquals(5, numRecords);
    }
  }

  @Test
  public void testFetchSize() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest(DEFAULT_TEST_TABLE));

    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setFetchSize(1)
        .connect(addr.hostname, addr.port);

    RecordServiceWorkerClient.TaskState handle = worker.execTask(plan.tasks.get(0));
    int numRecords = 0;
    while (true) {
      FetchResult result = worker.fetch(handle);
      numRecords += result.numRecords;
      assertTrue(result.numRecords == 0 || result.numRecords == 1);
      if (result.done) break;
    }
    assertEquals(25, numRecords);
    worker.close();
  }

  @Test
  public void testMemLimitExceeded() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setMemLimit(200L)
        .connect(addr.hostname, addr.port);

    RecordServiceWorkerClient.TaskState handle = worker.execTask(plan.tasks.get(0));
    boolean exceptionThrown = false;
    try {
      worker.fetch(handle);
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertEquals(RecordServiceException.ErrorCode.OUT_OF_MEMORY, e.code);
    }
    assertTrue(exceptionThrown);
    worker.closeTask(handle);

    // Try again going through the utility.
    exceptionThrown = false;
    try {
      worker.execAndFetch(plan.tasks.get(0));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertEquals(RecordServiceException.ErrorCode.OUT_OF_MEMORY, e.code);
    }
    assertTrue(exceptionThrown);

    assertEquals(0, worker.numActiveTasks());
    worker.close();
  }

  @Test
  public void testEmptyProjection() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createProjectionRequest(DEFAULT_TEST_TABLE, null));
    assertEquals(1, plan.tasks.size());

    // Verify schema
    assertEquals(1, plan.schema.cols.size());
    assertEquals("count(*)", plan.schema.cols.get(0).name);
    assertEquals(Schema.Type.BIGINT, plan.schema.cols.get(0).type.typeId);

    // Verify count(*) result.
    Records records = WorkerClientUtil.execTask(plan, 0);
    int count = 0;
    while (records.hasNext()) {
      records.next();
      ++count;
    }
    assertEquals(25, count);
    records.close();

    // Empty column list should do the same
    plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createProjectionRequest(DEFAULT_TEST_TABLE, new ArrayList<String>()));
    assertEquals(1, plan.tasks.size());

    plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(
                String.format("select count(*), count(*) from %s", DEFAULT_TEST_TABLE)));

    assertEquals(1, plan.tasks.size());
    assertEquals(2, plan.schema.cols.size());
    assertEquals("count(*)", plan.schema.cols.get(0).name);
    assertEquals("count(*)", plan.schema.cols.get(1).name);
    assertEquals(Schema.Type.BIGINT, plan.schema.cols.get(0).type.typeId);
    assertEquals(Schema.Type.BIGINT, plan.schema.cols.get(1).type.typeId);
  }

  @Test
  public void testProjection() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createProjectionRequest(
            DEFAULT_TEST_TABLE, Lists.newArrayList("n_comment")));
    assertEquals(1, plan.tasks.size());
    assertEquals(1, plan.schema.cols.size());
    assertEquals("n_comment", plan.schema.cols.get(0).name);
    assertEquals(Schema.Type.STRING, plan.schema.cols.get(0).type.typeId);
  }

  @Test
  public void testLimit() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);

    // Set with a higher than count limit.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setLimit(30L)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLimit(null)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLimit(10L)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 10);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLimit(1L)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 1);
    worker.close();
  }

  @Test
  public void testServerLoggingLevels() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    assertEquals(1, plan.tasks.size());
    NetworkAddress addr = plan.tasks.get(0).localHosts.get(0);

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setLoggingLevel(LoggingLevel.ALL)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLoggingLevel(LoggerFactory.getLogger(TestBasicClient.class))
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();

    worker = new RecordServiceWorkerClient.Builder()
        .setLoggingLevel((Logger)null)
        .connect(addr.hostname, addr.port);
    fetchAndVerifyCount(worker.execAndFetch(plan.tasks.get(0)), 25);
    worker.close();
  }

  @Test
  public void testNonLocalWorker() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    assertEquals(1, plan.tasks.size());

    // Clear the local hosts.
    Task task = plan.tasks.get(0);
    task.localHosts.clear();
    Records records = WorkerClientUtil.execTask(plan, 0);
    fetchAndVerifyCount(records, 25);
    records.close();

    // Clear all hosts.
    boolean exceptionThrown = false;
    plan.hosts.clear();
    try {
      WorkerClientUtil.execTask(plan, 0);
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("No hosts are provided"));
    }
    assertTrue(exceptionThrown);

    // Try invalid task id
    exceptionThrown = false;
    try {
      WorkerClientUtil.execTask(plan, 1);
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Invalid task id."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      WorkerClientUtil.execTask(plan, -1);
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Invalid task id."));
    }
    assertTrue(exceptionThrown);
  }

  private final static class MultithreadedTestState {
    public volatile boolean failed = false;
    public Throwable firstException = null;
    public int numSuccess = 0;

    public void fail(Throwable e) {
      synchronized (this) {
        failed = true;
        if (firstException == null) {
          firstException = e;
        }
      }
    }
  }

  @Test
  // Collects all the tests and runs them multithreaded.
  // TODO: can this just be done with JUnit?
  public void testAllMultithreaded() throws InterruptedException {
    List<Method> methods = new ArrayList<Method>();
    for (Method m: getClass().getMethods()) {
      if (m.getAnnotation(org.junit.Test.class) != null) {
        if (m.getName().equals("testAllMultithreaded")) continue;
        // These two are flaky - it seems the timeout we set (1ms) is still
        // too large sometimes, and the tests will fail sometimes.
        // TODO: improve these two tests and re-enable them.
        if (m.getName().equals("testPlannerTimeout")) continue;
        if (m.getName().equals("testWorkerTimeout")) continue;
        // This one fails for the VM and the assumption that we use to skip
        // it causes this entire test to fail. This is another reason that
        // we should try to use junit to run these multithreaded instead
        // of this homegrown solution.
        if (m.getName().equals("testAllTypesPathWithSchema")
            && RECORD_SERVICE_QUICKSTART_VM) continue;
        for (int i = 0; i < MULTITHREADED_TEST_NUM_ITERATIONS; ++i) {
          methods.add(m);
        }
      }
    }

    // Randomize the method list.
    Collections.shuffle(methods);

    ExecutorService executor = Executors.newFixedThreadPool(
        MULTITHREADED_TEST_NUM_THREADS);
    final MultithreadedTestState state = new MultithreadedTestState();
    for (final Method m: methods) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          if (state.failed) return;
          try {
            m.invoke(TestBasicClient.this);
            synchronized (state) {
              ++state.numSuccess;
            }
          } catch (IllegalAccessException e) {
            state.fail(e);
          } catch (IllegalArgumentException e) {
            state.fail(e);
          } catch (InvocationTargetException e) {
            state.fail(e.getTargetException() != null ? e.getTargetException() : e);
          }
        }
      });
    }
    executor.shutdown();
    while (!executor.isTerminated()) {
      Thread.sleep(1000);
    }
    assertFalse(state.firstException == null ? "Test failed " :
        state.firstException.toString(), state.failed);
    assertEquals(methods.size(), state.numSuccess);
  }
}
