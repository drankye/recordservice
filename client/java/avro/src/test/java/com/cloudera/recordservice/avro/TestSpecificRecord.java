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

package com.cloudera.recordservice.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.recordservice.avro.RecordUtil.ResolveBy;
import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.TestBase;
import com.cloudera.recordservice.core.WorkerClientUtil;

public class TestSpecificRecord extends TestBase {
  @Test
  public void testNationAll() throws RecordServiceException, IOException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from tpch.nation"));

    assertEquals(1, plan.tasks.size());
    SpecificRecords<NationAll> records = null;
    try {
      records = new SpecificRecords<NationAll>(NationAll.SCHEMA$,
          WorkerClientUtil.execTask(plan, 0), ResolveBy.ORDINAL);
      int numRecords = 0;
      while (records.hasNext()) {
        NationAll record = records.next();
        ++numRecords;
        if (numRecords == 3) {
          assertEquals(2, record.getKey().intValue());
          assertEquals("BRAZIL", record.getName());
          assertEquals(1, record.getRegionKey().intValue());
          assertEquals("y alongside of the pending deposits. carefully special " +
              "packages are about the ironic forges. slyly special ",
              record.getComment());
        }
      }
      assertEquals(25, numRecords);
    } finally {
      if (records != null) records.close();
    }
  }

  @Test
  public void testNationProjection() throws RecordServiceException, IOException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select n_nationkey, n_name from tpch.nation"));

    assertEquals(1, plan.tasks.size());
    SpecificRecords<NationKeyName> records = null;
    try {
      records = new SpecificRecords<NationKeyName>(NationKeyName.SCHEMA$,
          WorkerClientUtil.execTask(plan, 0), ResolveBy.NAME);
      int numRecords = 0;
      while (records.hasNext()) {
        NationKeyName record = records.next();
        ++numRecords;
        if (numRecords == 4) {
          assertEquals("CANADA", record.getNName());
          assertEquals(3, record.getNNationkey().intValue());
        }
      }
      assertEquals(25, numRecords);
    } finally {
      if (records != null) records.close();
    }
  }

  /**
   * Test the case when writer schema does not contain columns in reader schema,
   * while reader schema does not have default value.
   */
  @Test
  public void testInvalidExtraCols() throws RecordServiceException, IOException{
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from rs.users"));
    SpecificRecords<UserWithInvalidExtraCols> records = null;
    assertEquals(6, plan.tasks.size());
    boolean exceptionThrown = false;
    for (int i = plan.tasks.size() - 1; i >= 0; --i) {
      try {
        records = new SpecificRecords<UserWithInvalidExtraCols>(
            UserWithInvalidExtraCols.SCHEMA$, WorkerClientUtil.execTask(plan, i),
            ResolveBy.NAME);
        // test records with columns without default value: string and union
        records.next();
      } catch (RuntimeException e) {
        exceptionThrown = true;
        assertTrue(e.getMessage().contains("Default value is not set"));
      } finally {
        assertEquals(true, exceptionThrown);
        exceptionThrown = false;
        if (records != null) records.close();
      }
    }
  }

  /**
   * Test the case when writer schema does not contain columns in reader schema,
   * while reader schema has default value for these columns.
   */
  @Test
  public void testValidExtraCols() throws RecordServiceException, IOException{
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from rs.users"));
    SpecificRecords<UserWithValidExtraCols> records = null;
    assertEquals(6, plan.tasks.size());
    for (int i = plan.tasks.size() - 1; i >= 0; --i) {
      try {
        records = new SpecificRecords<UserWithValidExtraCols>(
            UserWithValidExtraCols.SCHEMA$, WorkerClientUtil.execTask(plan, i),
            ResolveBy.NAME);

        UserWithValidExtraCols record = records.next();
        // test int with default value = 10
        assertEquals(10, record.getDefaultNum().intValue());
        // test union with default value = null
        assertEquals(null, record.getDefaultUnion());
      } finally {
        if (records != null) records.close();
      }
    }
  }
}
