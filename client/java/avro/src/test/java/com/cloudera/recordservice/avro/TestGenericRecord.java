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
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.TestBase;
import com.cloudera.recordservice.core.WorkerClientUtil;

public class TestGenericRecord extends TestBase {
  @Test
  public void testNation() throws RecordServiceException, IOException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from tpch.nation"));

    // Verify schema
    Schema avroSchema = SchemaUtils.convertSchema(plan.schema);
    assertTrue(avroSchema.getName() == null);
    assertEquals(Schema.Type.RECORD, avroSchema.getType());
    List<Schema.Field> fields = avroSchema.getFields();
    assertEquals(4, fields.size());
    assertEquals("n_nationkey", fields.get(0).name());
    assertEquals(Schema.Type.INT, fields.get(0).schema().getType());
    assertEquals("n_name", fields.get(1).name());
    assertEquals(Schema.Type.STRING, fields.get(1).schema().getType());
    assertEquals("n_regionkey", fields.get(2).name());
    assertEquals(Schema.Type.INT, fields.get(2).schema().getType());
    assertEquals("n_comment", fields.get(3).name());
    assertEquals(Schema.Type.STRING, fields.get(3).schema().getType());

    // Execute the task
    assertEquals(1, plan.tasks.size());
    GenericRecords records = null;
    try {
      records = new GenericRecords(WorkerClientUtil.execTask(plan, 0));
      int numRecords = 0;
      while (records.hasNext()) {
        GenericData.Record record = records.next();
        ++numRecords;
        if (numRecords == 2) {
          assertEquals(record.get(0), record.get("n_nationkey"));
          assertEquals(record.get(1), record.get("n_name"));
          assertEquals(record.get(2), record.get("n_regionkey"));
          assertEquals(record.get(3), record.get("n_comment"));

          assertEquals(1, record.get("n_nationkey"));
          assertEquals("ARGENTINA", record.get("n_name"));
          assertEquals(1, record.get("n_regionkey"));
          assertEquals("al foxes promise slyly according to the regular accounts. " +
              "bold requests alon", record.get("n_comment"));
        }
      }
      assertEquals(25, numRecords);
    } finally {
      if (records != null) records.close();
    }
  }
}
