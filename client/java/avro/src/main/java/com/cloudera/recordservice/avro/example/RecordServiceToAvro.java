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

package com.cloudera.recordservice.avro.example;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import com.cloudera.recordservice.avro.GenericRecords;
import com.cloudera.recordservice.avro.SchemaUtils;
import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.WorkerClientUtil;

/**
 * Example utility that converts results returned from the RecordService
 * as avro, output as json.
 */
public class RecordServiceToAvro {
  static final String PLANNER_HOST = System.getenv("RECORD_SERVICE_PLANNER_HOST") != null ?
      System.getenv("RECORD_SERVICE_PLANNER_HOST") : "localhost";
  static final int PLANNER_PORT = System.getenv("RECORD_SERVICE_PLANNER_PORT") != null ?
      Integer.parseInt(System.getenv("RECORD_SERVICE_PLANNER_PORT")) : 12050;

  public static void main(String[] args) throws RecordServiceException, IOException {
    String query = "select * from tpch.nation";
    if (args.length == 2) query = args[1];

    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(query));
    Schema avroSchema = SchemaUtils.convertSchema(plan.schema);
    System.out.println("Avro Schema:\n" + avroSchema);

    System.out.println("Records:");
    for (int t = 0; t < plan.tasks.size(); ++t) {
      GenericRecords records = null;
      try {
        records = new GenericRecords(WorkerClientUtil.execTask(plan, t));
        while (records.hasNext()) {
          GenericData.Record record = records.next();
          System.out.println(record);
        }
      } finally {
        if (records != null) records.close();
      }
    }
  }
}
