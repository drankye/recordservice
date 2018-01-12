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

package com.cloudera.recordservice.examples;

import java.io.IOException;

import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Records.Record;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.Schema;
import com.cloudera.recordservice.core.WorkerClientUtil;

/**
 * This benchmarks a query of the form "select sum(first_col) from table". The request
 * pushed to RecordService is "select <cols> from table" and the sum is done in this
 * application.
 *
 * The intent of this application is to measure the server and client library record
 * reading performance.
 */
public class SumQueryBenchmark {
  static final String PLANNER_HOST =
    System.getenv("RECORD_SERVICE_PLANNER_HOST") != null ?
        System.getenv("RECORD_SERVICE_PLANNER_HOST") : "localhost";
  static final int PLANNER_PORT =
    System.getenv("RECORD_SERVICE_PLANNER_PORT") != null ?
        Integer.parseInt(System.getenv("RECORD_SERVICE_PLANNER_PORT")) : 12050;

  static final String DEFAULT_QUERY = "select bigint_col from rs.alltypes";

  private static void runQuery(String query) throws RecordServiceException, IOException {
    /**
     * First talk to the plan service to get the list of tasks.
     */
    System.out.println("Running request: " + query);

    PlanRequestResult planResult = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(query));
    if (planResult.schema.cols.size() < 1 ||
        planResult.schema.cols.get(0).type.typeId != Schema.Type.BIGINT) {
      throw new RuntimeException(
          "First column must be a BIGINT. Schema=" + planResult.schema);
    }

    long totalTimeMs = 0;

    int totalRows = 0;
    long sum = 0;

    // Run each task and fetch results until we're done
    for (int i = 0; i < planResult.tasks.size(); ++i) {
      long start = System.currentTimeMillis();
      Records records = null;
      try {
        planResult.tasks.get(i).setTag("SumQueryBenchmark");
        records = WorkerClientUtil.execTask(planResult, i);
        while (records.hasNext()) {
          Record record = records.next();
          sum += record.nextLong(0);
          ++totalRows;
        }
      } finally {
        if (records != null) records.close();
      }
      totalTimeMs += System.currentTimeMillis() - start;
    }

    System.out.println("Task complete. Returned: " + totalRows + " rows.");
    System.out.println("Sum: " + sum);
    System.out.println("Took " + totalTimeMs + "ms");
  }

  public static void main(String[] args) throws RecordServiceException, IOException {
    String query = DEFAULT_QUERY;
    if (args.length > 0) query = args[0];
    runQuery(query);
  }

}
