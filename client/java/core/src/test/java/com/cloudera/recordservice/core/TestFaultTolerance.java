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

import java.io.IOException;

import org.junit.Test;

// Tests fault tolerance and retry logic in the client library.
public class TestFaultTolerance extends TestBase {
  @Test
  public void testWorkerRetry() throws RuntimeException, IOException,
        RecordServiceException, InterruptedException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
        .setMaxAttempts(3).setSleepDurationMs(10).setFetchSize(1)
        .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);
    Records records = worker.execAndFetch(plan.tasks.get(0));
    int numRecords = 0;
    while (records.hasNext()) {
      if (numRecords % 2 == 0) {
        // Close the underlying connection. This simulates a failure where the
        // worker does not die but the connection is dropped.
        worker.closeConnectionForTesting();
      }
      Records.Record record = records.next();
      assertEquals(numRecords, record.nextShort(0));
      ++numRecords;
    }

    assertEquals(25, numRecords);

    worker.close();
  }

  @Test
  public void testPlannerRetry() throws RuntimeException, IOException,
          RecordServiceException, InterruptedException {
    RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
        .setMaxAttempts(3).setSleepDurationMs(10)
        .connect(PLANNER_HOST, PLANNER_PORT);

    planner.closeConnectionForTesting();
    boolean exceptionThrown = false;
    try {
      planner.planRequest(Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);

    planner.closeConnectionForTesting();
    exceptionThrown = false;
    try {
      planner.getSchema(Request.createTableScanRequest(DEFAULT_TEST_TABLE));
    } catch (RecordServiceException e) {
      exceptionThrown = true;
    }
    assertFalse(exceptionThrown);

    planner.close();
  }
}
