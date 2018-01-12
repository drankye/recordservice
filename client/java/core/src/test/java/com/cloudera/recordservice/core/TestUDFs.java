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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.io.IOException;

/**
 * Test valid and invalid UDFs.
 */
public class TestUDFs extends TestBase {

  @Test
  public void testNotExistUDF() throws IOException {
    RecordServicePlannerClient planner = null;
    boolean exceptionThrown = false;

    // Fail the request and throw the 'unknown' exception if the UDF does not exist.
    try {
      planner = new RecordServicePlannerClient.Builder()
          .connect(PLANNER_HOST, PLANNER_PORT);
      planner.planRequest(Request.createSqlRequest(
          String.format("select undefined_udf(n_name) from %s", DEFAULT_TEST_TABLE)));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.code == RecordServiceException.ErrorCode.INVALID_REQUEST);
      assertTrue(e.getMessage().contains("default.undefined_udf() unknown"));
    } finally {
      if (planner != null) {
        planner.close();
      }
      assertTrue(exceptionThrown);
    }

    // Fail the request and throw the 'No matching' exception if the UDF signature does
    // not exist.
    exceptionThrown = false;
    try {
      planner = new RecordServicePlannerClient.Builder()
          .connect(PLANNER_HOST, PLANNER_PORT);
      planner.planRequest(Request.createSqlRequest(
          String.format("select mask(n_name, 1) from %s", DEFAULT_TEST_TABLE)));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.code == RecordServiceException.ErrorCode.INVALID_REQUEST);
      assertTrue(e.getMessage().contains(
          "No matching function with signature: default.mask(STRING, TINYINT)."));
    } finally {
      if (planner != null) {
        planner.close();
      }
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testHiveJavaUDF() throws IOException, RecordServiceException {
    RecordServicePlannerClient planner = null;
    RecordServiceWorkerClient worker = null;
    try {
      planner = new RecordServicePlannerClient.Builder()
          .connect(PLANNER_HOST, PLANNER_PORT);
      // Mask the first character for column n_name
      PlanRequestResult result = planner.planRequest(Request.createSqlRequest(
          String.format("select mask(n_name, 0, 0) from %s", DEFAULT_TEST_TABLE)));
      assertTrue(result.tasks.size() == 1);
      NetworkAddress addr = result.tasks.get(0).localHosts.get(0);

      worker = new RecordServiceWorkerClient.Builder()
          .setLoggingLevel(LoggingLevel.ALL)
          .connect(addr.hostname, addr.port);
      Records records = worker.execAndFetch(result.tasks.get(0));
      while (records.hasNext()) {
        Records.Record r = records.next();
        assertTrue(r.nextByteArray(0).toString().startsWith("*"));
      }
    } finally {
      if (planner != null) {
        planner.close();
      }
      if (worker != null) {
        worker.close();
      }
    }
  }
}
