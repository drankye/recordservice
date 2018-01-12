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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

/**
 * This class tests the Sentry service with RecordService, and is
 * used in $RECORD_SERVICE_HOME/tests/run_sentry_tests.py.
 * Notice that this assumes that the test user, role, view, etc.,
 * have been set up by that script, before this test is executed.
 */
public class TestSentry extends TestBase {
  private static final boolean IGNORE_SENTRY_TESTS =
      System.getenv("IGNORE_SENTRY_TESTS") == null ||
      System.getenv("IGNORE_SENTRY_TESTS").equalsIgnoreCase("true");
  private final String PRIVILEGE_ERROR_MSG = "does not have privileges";

  @Test
  /**
   * Tests Sentry service with RecordService SQL request.
   */
  public void testSQLRequest() throws IOException, RecordServiceException {
    if (IGNORE_SENTRY_TESTS) return;

    // First, try to access the tpch.nation table, which the test role doesn't
    // have access to. It should fail with exception.
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);

    try {
      planner.planRequest(Request.createTableScanRequest(DEFAULT_TEST_TABLE));
      assertTrue("plan request should have thrown an exception", false);
    } catch (RecordServiceException ex) {
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
      assertTrue("Actual message is: " + ex.detail,
          ex.detail.contains(PRIVILEGE_ERROR_MSG));
    }

    // Should fail when accessing the columns which the test role doesn't have access to.
    try {
      planner.planRequest(Request.createSqlRequest(
          String.format("select n_regionkey, n_comment from %s", DEFAULT_TEST_TABLE)));
      assertTrue("plan request should have thrown an exception", false);
    } catch (RecordServiceException ex) {
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
      assertTrue("Actual message is: " + ex.detail,
          ex.detail.contains(PRIVILEGE_ERROR_MSG));
    }

    // Accessing columns which the test role has access to should work.
    planner.planRequest(Request.createSqlRequest(
        String.format("select n_name, n_nationkey from %s", DEFAULT_TEST_TABLE)));

    // Accessing tpch.nation_view should work
    planner.planRequest(Request.createTableScanRequest("tpch.nation_view"));
    planner.close();
  }

  @Test
  /**
   * Tests Sentry service with RecordService path request.
   */
  public void testPathRequest() throws IOException, RecordServiceException {
    if (IGNORE_SENTRY_TESTS) return;

    // First, try to access the /test-warehouse/tpch.orders, which the test role doesn't
    // have access to. It should fail with exception.
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .connect(PLANNER_HOST, PLANNER_PORT);

    try {
      planner.planRequest(Request.createPathRequest("/test-warehouse/tpch.orders"));
      assertTrue("plan request should have thrown an exception", false);
    } catch (RecordServiceException ex) {
      assertEquals(RecordServiceException.ErrorCode.AUTHENTICATION_ERROR, ex.code);
      assertTrue("Actual message is: " + ex.message,
          ex.message.contains("does not have full access to the path"));
    }

    // Accessing /test-warehouse/tpch.nation should work
    planner.planRequest(Request.createPathRequest("/test-warehouse/tpch.nation"));
    planner.close();
  }

  @Test
  /**
   * Access the udf and column which the test role has access to should work.
   */
  public void testUDFWithRequiredPrivileges() throws RecordServiceException, IOException {
    if (IGNORE_SENTRY_TESTS) return;

    RecordServicePlannerClient planner = null;
    try {
      planner = new RecordServicePlannerClient.Builder()
          .connect(PLANNER_HOST, PLANNER_PORT);
      planner.planRequest(Request.createSqlRequest(
          String.format("select udf.mask2(n_name, 1, 1) from %s", DEFAULT_TEST_TABLE)));
    } finally {
      if (planner != null) planner.close();
    }
  }

  @Test
  /**
   * If the test role doesn't have access to the database of the UDF, the request should
   * fail with exception.
   */
  public void testUDFWithInaccessibleDb() throws RecordServiceException, IOException {
    if (IGNORE_SENTRY_TESTS) return;

    RecordServicePlannerClient planner = null;
    boolean exceptionThrown = false;
    try {
      planner = new RecordServicePlannerClient.Builder()
          .connect(PLANNER_HOST, PLANNER_PORT);
      planner.planRequest(Request.createSqlRequest(
          String.format("select mask(n_name, 1, 1) from %s", DEFAULT_TEST_TABLE)));
    } catch (RecordServiceException ex) {
      exceptionThrown =  true;
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
      assertTrue("Actual message is: " + ex.getMessage(),
          ex.getMessage().contains(PRIVILEGE_ERROR_MSG));
    } finally {
      if (planner != null) planner.close();
      assertTrue(exceptionThrown);
    }
  }

  @Test
  /**
   * If the test role doesn't have access to the uri of the udf, the request should fail
   * with exception.
   */
  public void testUDFWithInaccessibleUri() throws RecordServiceException, IOException {
    if (IGNORE_SENTRY_TESTS) return;

    RecordServicePlannerClient planner = null;
    boolean exceptionThrown = false;
    try {
      planner = new RecordServicePlannerClient.Builder()
          .connect(PLANNER_HOST, PLANNER_PORT);
      planner.planRequest(Request.createSqlRequest(
          String.format("select udf.mask1(n_name, 1, 1) from %s", DEFAULT_TEST_TABLE)));
    } catch (RecordServiceException ex) {
      exceptionThrown =  true;
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
      assertTrue("Actual message is: " + ex.getMessage(),
          ex.getMessage().contains(PRIVILEGE_ERROR_MSG));
    } finally {
      if (planner != null) planner.close();
      assertTrue(exceptionThrown);
    }
  }

  @Test
  /**
   * If the test role has access to the uri of the udf, but doesn't have access to the
   * column, the request should fail with exception.
   */
  public void testUDFWithInaccessibleCol() throws RecordServiceException, IOException {
    if (IGNORE_SENTRY_TESTS) return;

    RecordServicePlannerClient planner = null;
    boolean exceptionThrown = false;
    try {
      planner = new RecordServicePlannerClient.Builder()
          .connect(PLANNER_HOST, PLANNER_PORT);
      planner.planRequest(Request.createSqlRequest(String.format(
          "select udf.mask2(n_comment, 1, 1) from %s", DEFAULT_TEST_TABLE)));
    } catch (RecordServiceException ex) {
      exceptionThrown =  true;
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
      assertTrue("Actual message is: " + ex.getMessage(),
          ex.getMessage().contains(PRIVILEGE_ERROR_MSG));
    } finally {
      if (planner != null) planner.close();
      assertTrue(exceptionThrown);
    }
  }
}
