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

import java.io.IOException;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestS3 extends TestBase {

  static final boolean RUN_S3_TESTS =
      System.getenv("S3") != null &&
      System.getenv("S3").equalsIgnoreCase("1");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestBase.setUpBeforeClass();
    if (RUN_S3_TESTS) {
      System.out.println("Running s3 tests.");
    } else {
      System.out.println("Skipping s3 tests.");
    }
  }

  @Test
  public void testNation() throws RecordServiceException, IOException {
    Assume.assumeTrue(RUN_S3_TESTS);

    // Plan the request
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from rs.s3_nation"));
    for (int i = 0; i < plan.tasks.size(); ++i) {
      assertEquals(0, plan.tasks.get(i).localHosts.size());
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
    }
  }
}
