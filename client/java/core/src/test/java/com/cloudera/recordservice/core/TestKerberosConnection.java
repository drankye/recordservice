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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests connect to kerberized cluster. This is not normally run as it involves
 * a non-trivial local set up to get tickets and what not.
 * TODO: add renew/expire tests.
 */
public class TestKerberosConnection extends TestBase {
  // Kerberized cluster.
  static final String KERBEROS_HOSTS = "KERBEROS_HOSTS";

  // Kerberos hosts (planners & workers), planner and planner principal for testing
  private static String[] kerberosHosts_;
  private static String plannerHost_;
  private static String plannerPrincipal_;

  // Number of rows in the sample_07 table.
  static final int SAMPLE_07_ROW_COUNT = 823;

  private static final boolean HAS_KERBEROS_CREDENTIALS =
      System.getenv("HAS_KERBEROS_CREDENTIALS") != null &&
      System.getenv("HAS_KERBEROS_CREDENTIALS").equalsIgnoreCase("true");

  // Check before each test and make sure all the requirements are satisfied
  @Before
  public void checkBeforeTest() {
    Assume.assumeTrue(HAS_KERBEROS_CREDENTIALS && kerberosHosts_ != null &&
        plannerHost_ != null && plannerPrincipal_ != null);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestBase.setUpBeforeClass();

    if (!HAS_KERBEROS_CREDENTIALS) {
      System.out.println("Skipping Kerberos tests.");
      return;
    }

    if (System.getenv(KERBEROS_HOSTS) != null) {
      kerberosHosts_ = System.getenv(KERBEROS_HOSTS).split(":");
      if (kerberosHosts_.length == 0) {
        System.out.println("Can't find any host from the input '"
            + KERBEROS_HOSTS + "': " + System.getenv(KERBEROS_HOSTS));
        return;
      }

      System.out.println(KERBEROS_HOSTS + ":");
      for (String host: kerberosHosts_) {
        System.out.println(host);
      }

      plannerHost_ = kerberosHosts_[0];
      plannerPrincipal_ = TestUtil.makePrincipal("impala", plannerHost_);
    } else {
      System.out.println("To run Kerberos tests, you need to set" +
          " environment variable '" + KERBEROS_HOSTS
          + "' with a colon separated list of kerberoized hosts.");
    }
  }

  @Test
  public void testConnection() throws IOException,
      RecordServiceException, InterruptedException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .planRequest(plannerHost_, PLANNER_PORT,
            Request.createTableScanRequest("sample_07"));

    assertEquals(4, plan.schema.cols.size());

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .connect(plannerHost_, DEFAULT_WORKER_PORT);
    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      records.next();
      ++numRecords;
    }
    assertEquals(SAMPLE_07_ROW_COUNT, numRecords);
    worker.close();
  }

  @Test
  // Test without providing a principal or a bad principal.
  public void testBadConnection() throws IOException,
        RecordServiceException, InterruptedException {
    // Try planner connection with no principal and bad principal
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .planRequest(plannerHost_, PLANNER_PORT,
              Request.createTableScanRequest("sample_07"));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect without kerberos principal",
        exceptionThrown);

    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal("BAD/bad.com@bad.com")
          .planRequest(plannerHost_, PLANNER_PORT,
              Request.createTableScanRequest("sample_07"));
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect with bad kerberos principal",
        exceptionThrown);

    // Try worker connection with no principal and bad principal
    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder()
          .connect(plannerHost_, DEFAULT_WORKER_PORT)
          .close();
    } catch (RecordServiceException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect without kerberos principal",
        exceptionThrown);

    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder()
          .setKerberosPrincipal("BAD/bad.com@bad.com")
          .connect(plannerHost_, DEFAULT_WORKER_PORT)
          .close();
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assertTrue("Should not be able to connect with bad kerberos principal",
        exceptionThrown);
  }

  @Test
  // Test authentication with delegation token.
  public void testDelegationToken() throws IOException,
        RecordServiceException, InterruptedException {
    boolean exceptionThrown = false;

    // Connect to the planner via kerberos.
    RecordServicePlannerClient kerberizedPlanner =
        new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .connect(plannerHost_, PLANNER_PORT);

    // Get a token from planner.
    DelegationToken token1 = kerberizedPlanner.getDelegationToken("impala");
    assertTrue(token1.identifier.length() > 0);
    assertTrue(token1.password.length() > 0);
    assertTrue(token1.token.length > token1.identifier.length());
    assertTrue(token1.token.length > token1.password.length());

    // Get a second token
    DelegationToken token2 = kerberizedPlanner.getDelegationToken("impala");

    // Renew the token.
    kerberizedPlanner.renewDelegationToken(token1);
    kerberizedPlanner.close();

    // Connect to the planner using the token.
    RecordServicePlannerClient tokenPlanner = new RecordServicePlannerClient.Builder()
        .setDelegationToken(token1).connect(plannerHost_, PLANNER_PORT);

    // Should only be able to get tokens if the connection is kerberized.
    try {
      tokenPlanner.getDelegationToken(null);
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(
          "can only be called with a Kerberos connection."));
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      tokenPlanner.renewDelegationToken(token1);
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(
          "can only be called with a Kerberos connection."));
    }
    assertTrue(exceptionThrown);

    // But other APIs should work.
    PlanRequestResult plan = tokenPlanner.planRequest(
        Request.createTableScanRequest("sample_07"));
    assertTrue(plan.tasks.size() == 1);

    // Try a new request (this creates a new connection).
    new RecordServicePlannerClient.Builder().setDelegationToken(token1)
        .getSchema(plannerHost_, PLANNER_PORT,
            Request.createTableScanRequest("sample_07"));
    // Try with other token.
    new RecordServicePlannerClient.Builder().setDelegationToken(token2)
        .getSchema(plannerHost_, PLANNER_PORT,
            Request.createTableScanRequest("sample_07"));

    // Create a worker connection with the token.
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder().
        setDelegationToken(token1).connect(plannerHost_, DEFAULT_WORKER_PORT);

    // Fetch the results.
    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      records.next();
      ++numRecords;
    }
    assertEquals(SAMPLE_07_ROW_COUNT, numRecords);
    worker.close();

    // Cancel the token. Note that this can be done without a kerberized connection.
    tokenPlanner.cancelDelegationToken(token1);
    tokenPlanner.close();

    // Shouldn't be able to connect with it anymore.
    exceptionThrown = false;

    try {
      new RecordServiceWorkerClient.Builder().setDelegationToken(token1)
          .connect(plannerHost_, DEFAULT_WORKER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
      // TODO: the error is generated deep in the sasl negotiation and we
      // don't get a generic error. Fix this.
    }
    assertTrue(exceptionThrown);

    // Try to connect with the canceled token. Should fail.
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(token1)
          .connect(plannerHost_, PLANNER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
      // TODO: the error is generated deep in the sasl negotiation and we
      // don't get a generic error. Fix this.
    }
    assertTrue(exceptionThrown);

    // Token2 should still work (not cancelled).
    new RecordServicePlannerClient.Builder().setDelegationToken(token2)
        .getSchema(plannerHost_, PLANNER_PORT,
            Request.createTableScanRequest("sample_07"));
    new RecordServiceWorkerClient.Builder().setDelegationToken(token2)
        .connect(plannerHost_, DEFAULT_WORKER_PORT).close();
  }

  @Test
  public void testInvalidToken() throws IOException,
      RecordServiceException, InterruptedException {
    boolean exceptionThrown = false;

    // Connect to the planner via kerberos.
    RecordServicePlannerClient kerberizedPlanner =
        new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .connect(plannerHost_, PLANNER_PORT);

    // Get two tokens from planner.
    DelegationToken token1 = kerberizedPlanner.getDelegationToken("impala");
    DelegationToken token2 = kerberizedPlanner.getDelegationToken("impala");
    kerberizedPlanner.close();

    // Verify they work.
    new RecordServicePlannerClient.Builder()
        .setDelegationToken(token1).connect(plannerHost_, PLANNER_PORT).close();
    new RecordServicePlannerClient.Builder()
        .setDelegationToken(token2).connect(plannerHost_, PLANNER_PORT).close();

    DelegationToken testToken = new DelegationToken("", "", new byte[10]);
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
          .connect(plannerHost_, PLANNER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assert(exceptionThrown);

    // Set the identifier but no password
    testToken = new DelegationToken(token1.identifier, "", new byte[10]);
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
          .connect(plannerHost_, PLANNER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assert(exceptionThrown);

    // Set it to the other (wrong password);
    testToken = new DelegationToken(token1.identifier, token2.password, new byte[10]);
    exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
          .connect(plannerHost_, PLANNER_PORT).close();
    } catch (IOException e) {
      exceptionThrown = true;
    }
    assert(exceptionThrown);

    // Set it to the right password. Everything should still work.
    testToken = new DelegationToken(token1.identifier, token1.password, new byte[10]);
    new RecordServicePlannerClient.Builder().setDelegationToken(testToken)
        .connect(plannerHost_, PLANNER_PORT).close();
    new RecordServicePlannerClient.Builder().setDelegationToken(token1)
        .connect(plannerHost_, PLANNER_PORT).close();
    new RecordServicePlannerClient.Builder().setDelegationToken(token2)
        .connect(plannerHost_, PLANNER_PORT).close();
  }

  // Tests that a secure client connecting to an unsecure server behaves
  // reasonably.
  @Test
  public void testUnsecureConnection() throws IOException,
      RecordServiceException, InterruptedException {
    boolean exceptionThrown = false;

    // Try to connect to a unsecure server with a principal. This should fail.
    try {
      new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal(plannerPrincipal_)
          .setConnectionTimeoutMs(1000)
          .connect(PLANNER_HOST, PLANNER_PORT)
          .close();
    } catch (IOException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(
          "Ensure the server has security enabled."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    // Try to connect to a unsecure server with a principal. This should fail.
    exceptionThrown = false;
    try {
      new RecordServiceWorkerClient.Builder()
          .setKerberosPrincipal(plannerPrincipal_)
          .setConnectionTimeoutMs(1000)
          .connect(PLANNER_HOST, DEFAULT_WORKER_PORT)
          .close();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(
          "Ensure the server has security enabled."));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  // Tests that tokens are distributed across the cluster.
  @Test
  public void testPersistedTokens() throws IOException,
      RecordServiceException, InterruptedException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .connect(plannerHost_, PLANNER_PORT);
    for (String hostToCancel: kerberosHosts_) {
      DelegationToken token = planner.getDelegationToken("impala");
      // Wait for the token to go through the cluster.
      Thread.sleep(10000);

      // Try all the connections, they should all work.
      for (String host: kerberosHosts_) {
        new RecordServicePlannerClient.Builder().setDelegationToken(token)
            .setConnectionTimeoutMs(60000)
            .connect(host, PLANNER_PORT).close();
        new RecordServiceWorkerClient.Builder().setDelegationToken(token)
            .setConnectionTimeoutMs(60000)
            .connect(host, DEFAULT_WORKER_PORT).close();
      }

      // Cancel the token.
      RecordServicePlannerClient client = new RecordServicePlannerClient.Builder()
          .setDelegationToken(token)
          .setConnectionTimeoutMs(60000)
          .connect(hostToCancel, PLANNER_PORT);
      client.cancelDelegationToken(token);
      client.close();
      Thread.sleep(10000);

      // Try all the connections, they should all fail now.
      for (String host: kerberosHosts_) {
        boolean exceptionThrown = false;
        try {
          new RecordServicePlannerClient.Builder().setDelegationToken(token)
              .connect(host, PLANNER_PORT).close();
        } catch (IOException e) {
          exceptionThrown = true;
          assertTrue(e.getMessage().contains(
              "Could not connect to RecordServicePlanner"));
        }
        assertTrue(exceptionThrown);

        exceptionThrown = false;
        try {
          new RecordServiceWorkerClient.Builder().setDelegationToken(token)
              .connect(host, DEFAULT_WORKER_PORT).close();
        } catch (IOException e) {
          exceptionThrown = true;
          assertTrue(e.getMessage().contains(
              "Could not connect to RecordServiceWorker"));
        }
        assertTrue(exceptionThrown);
      }
    }
    planner.close();
  }

  @Test
  public void testEncryptedTasks() throws IOException, RecordServiceException {
    // Testing the case that request is planned on a non-secure cluster and executed
    // on a secure cluster
    PlanRequestResult result = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest("tpch.nation"));

    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .connect(plannerHost_, DEFAULT_WORKER_PORT);

    boolean exceptionThrown = false;
    try {
      worker.execTask(result.tasks.get(0));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertEquals(RecordServiceException.ErrorCode.AUTHENTICATION_ERROR, e.code);
      assertTrue(e.getMessage().contains(
          "Kerberos is enabled but task is not encrypted"));
    }

    assertTrue(exceptionThrown);
    worker.close();

    // Testing the case that request is planned on a secure cluster and executed
    // on a non-secure cluster
    result = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .planRequest(plannerHost_, PLANNER_PORT,
            Request.createTableScanRequest("sample_07"));

    // The table actually doesn't exist on the local box, but it doesn't
    // matter since the test should fail before the task queries the table.
    worker = new RecordServiceWorkerClient.Builder()
        .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);

    exceptionThrown = false;
    try {
      worker.execTask(result.tasks.get(0));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertEquals(RecordServiceException.ErrorCode.AUTHENTICATION_ERROR, e.code);
      assertTrue(e.getMessage().contains(
          "Kerberos is not enabled but task is encrypted."));
    }

    assertTrue(exceptionThrown);
    worker.close();

    // Testing the case where a task binary is modified, and thus should
    // fail the HMAC test on the worker.
    result = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .planRequest(plannerHost_, PLANNER_PORT,
            Request.createTableScanRequest("sample_07"));

    worker = new RecordServiceWorkerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .connect(plannerHost_, DEFAULT_WORKER_PORT);

    Random rand = new Random();
    byte[] byteArray = result.tasks.get(0).task;

    // Here we swap some random byte with the last byte in the buffer, to make
    // the task fail authentication. Retry several times since the random byte
    // may not necessarily be in the task binary.
    for (int i = 0; i < 50; ++i) {
      int idx = rand.nextInt(byteArray.length);
      byte b = byteArray[idx];
      byteArray[idx] = byteArray[byteArray.length-1];
      byteArray[byteArray.length-1] = b;

      exceptionThrown = false;
      try {
        worker.execTask(result.tasks.get(0));
      } catch (RecordServiceException e) {
        // We could get error during deserialization. In that case, keep retrying.
        if (e.getMessage().contains("Task is corrupt.")) continue;
        exceptionThrown = true;
        assertEquals(RecordServiceException.ErrorCode.INVALID_TASK, e.code);
        assertTrue(e.getMessage().contains("Task failed authentication."));
        break;
      }
    }

    assertTrue(exceptionThrown);
    worker.close();
  }

  /**
   * Test the case when there are multiple planners simultaneously generating and using
   * delegation tokens.
   * Note, this test assumes that all kerberos hosts that this test is using are
   * running RS planner.
   */
  @Test
  public void testMultiplePlanners() throws ExecutionException, InterruptedException,
      RecordServiceException, IOException {
    List<Future<Void>> futures = new ArrayList<Future<Void>>();
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    final Random rand = new Random();

    for (int i = 0; i < 20; ++i) {
      futures.add(executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          int idx = rand.nextInt(kerberosHosts_.length);
          RecordServicePlannerClient plannerClient =
              new RecordServicePlannerClient.Builder()
                  .setKerberosPrincipal(
                      TestUtil.makePrincipal("impala", kerberosHosts_[idx]))
                  .connect(kerberosHosts_[idx], PLANNER_PORT);

          // Try to get a delegation token
          DelegationToken token = null;
          try {
            token = plannerClient.getDelegationToken("impala");
          } catch (Exception ex) {
            assertFalse("getDelegationToken() failed with " + ex.getMessage(), true);
          }

          // Now randomly pick another planner and connect using the token, it should
          // be OK.
          idx = rand.nextInt(kerberosHosts_.length);
          try {
            plannerClient =
                new RecordServicePlannerClient.Builder()
                    .setDelegationToken(token)
                    .connect(kerberosHosts_[idx], PLANNER_PORT);
          } catch (Exception ex) {
            assertFalse("setDelegationToken() failed with " + ex.getMessage(), true);
          }
          return null;
        }
      }));
    }

    Throwable firstFailure = null;
    for (Future<Void> f : futures) {
      try {
        f.get();
      } catch (ExecutionException ex) {
        firstFailure = ex.getCause();
      }
    }

    executorService.shutdownNow();
    while (!executorService.isTerminated()) {
      Thread.sleep(1000);
    }

    if (firstFailure != null) {
      assertTrue("Test failed with: " + firstFailure.getMessage(), false);
    }
  }

  /**
   * Test accessing data within a HDFS encryption zone.
   */
  @Test
  public void testHdfsEncryption() throws RecordServiceException, IOException {
    RecordServicePlannerClient planner = null;
    RecordServiceWorkerClient worker = null;

    // Test table with location in a HDFS encryption zone.
    // Here table hdfs_encryp is stored as TEXTFILE with location in an encryption zone.
    try {
      planner = new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal(plannerPrincipal_)
          .connect(plannerHost_, PLANNER_PORT);
      PlanRequestResult result = planner
          .planRequest(Request.createSqlRequest("select * from hdfs_encryp"));
      assertTrue(result.tasks.size() == 1);

      NetworkAddress addr = result.tasks.get(0).localHosts.get(0);
      worker = new RecordServiceWorkerClient.Builder()
          .setKerberosPrincipal(TestUtil.makePrincipal("impala", addr.hostname))
          .connect(addr.hostname, addr.port);
      worker.execAndFetch(result.tasks.get(0));
    } finally {
      if (planner != null) {
        planner.close();
      }
      if (worker != null) {
        worker.close();
      }
    }

    // Test path within a HDFS encryption zone.
    try {
      planner = new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal(plannerPrincipal_)
          .connect(plannerHost_, PLANNER_PORT);
      PlanRequestResult result = planner.planRequest(
          Request.createPathRequest("/tmp/testzone"));
      assertTrue(result.tasks.size() == 1);

      NetworkAddress addr = result.tasks.get(0).localHosts.get(0);
      worker = new RecordServiceWorkerClient.Builder()
          .setKerberosPrincipal(TestUtil.makePrincipal("impala", addr.hostname))
          .connect(addr.hostname, addr.port);
      worker.execAndFetch(result.tasks.get(0));
    } finally {
      if (planner != null) {
        planner.close();
      }
      if (worker != null) {
        worker.close();
      }
    }
  }

  /**
   * Test path request which the current user doesn't have permission to access.
   */
  @Test
  public void testNoPermissionPath() throws RecordServiceException, IOException {
    boolean exceptionThrown = false;
    try {
      // test no permission path in hdfs
      new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal(plannerPrincipal_)
          .planRequest(plannerHost_, PLANNER_PORT,
              Request.createPathRequest("/tmp/testdir"));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, e.code);
      assertTrue(e.getMessage().contains("Could not list directory"));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      // Test no permission path within a hdfs encryption zone
      new RecordServicePlannerClient.Builder()
          .setKerberosPrincipal(plannerPrincipal_)
          .planRequest(plannerHost_, PLANNER_PORT,
              Request.createPathRequest("/tmp/huezone"));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, e.code);
      assertTrue(e.getMessage().contains("Could not list directory"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }
}
