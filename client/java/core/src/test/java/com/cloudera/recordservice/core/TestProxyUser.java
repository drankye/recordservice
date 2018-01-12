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

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Tests proxy user in kerberized cluster. This is not normally run as it involves
 * a non-trivial set up, such as authorized_proxy_user_config in server side, Kerberos
 * ticket in test node and etc.
 */
public class TestProxyUser extends TestBase{
  private static String proxyUser_;
  private static final String PROXY_USER = "PROXY_USER";
  private static String plannerHost_;
  private static String plannerPrincipal_;

  private static final boolean TEST_PROXY_USER =
      System.getenv("TEST_PROXY_USER") != null &&
      System.getenv("TEST_PROXY_USER").equalsIgnoreCase("true");

  // Check before each test and make sure all the requirements are satisfied
  @Before
  public void checkBeforeTest() {
    Assume.assumeTrue(TEST_PROXY_USER && proxyUser_ != null &&
        plannerHost_ != null && plannerPrincipal_ != null);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestBase.setUpBeforeClass();

    if (!TEST_PROXY_USER) {
      System.out.println("Skipping Proxy User tests.");
      return;
    }

    if (System.getenv(PROXY_USER) != null) {
      proxyUser_ = System.getenv(PROXY_USER);
    } else {
      System.out.println("To run Proxy User tests, you need to set" +
          " environment variable " + PROXY_USER);
    }

    if (System.getenv(TestKerberosConnection.KERBEROS_HOSTS) != null) {
      String[] kerberosHosts_ =
          System.getenv(TestKerberosConnection.KERBEROS_HOSTS).split(":");
      if (kerberosHosts_.length == 0) {
        System.out.println("Can't find any host from the input '"
            + TestKerberosConnection.KERBEROS_HOSTS + "': " +
            System.getenv(TestKerberosConnection.KERBEROS_HOSTS));
        return;
      }

      plannerHost_ = kerberosHosts_[0];
      plannerPrincipal_ = TestUtil.makePrincipal("impala", plannerHost_);
    } else {
      System.out.println("To run Proxy User tests, you need to set" +
          " environment variable '" + TestKerberosConnection.KERBEROS_HOSTS
          + "' with a colon separated list of kerberoized hosts.");
    }
  }

  @Test
  public void testProxyUser () throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .setKerberosPrincipal(plannerPrincipal_)
        .setDelegatedUser(proxyUser_)
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
    assertEquals(TestKerberosConnection.SAMPLE_07_ROW_COUNT, numRecords);
    worker.close();
  }
}
