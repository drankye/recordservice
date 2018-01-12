// Copyright 2012 Cloudera Inc.
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

#include <gtest/gtest.h>
#include <stdio.h>
#include <exception>

#include "test-common.h"

using namespace boost;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace recordservice;

const char* PLANNER_HOST = "localhost";
const int RECORD_SERVICE_PLANNER_PORT = 12050;
const int RECORD_SERVICE_WORKER_PORT = 13050;

static struct {
  int16_t c0;
  string c1;
  int16_t c2;
  string c3;
} NATION_TBL[] = {
  {0, "ALGERIA", 0, " haggle. carefully final deposits detect slyly agai" },
};

TEST(ClientTest, Nation) {
  try {
    shared_ptr<RecordServicePlannerClient> planner =
        CreatePlannerConnection(PLANNER_HOST, RECORD_SERVICE_PLANNER_PORT);
    shared_ptr<RecordServiceWorkerClient> worker =
        CreateWorkerConnection(PLANNER_HOST, RECORD_SERVICE_WORKER_PORT);

    TPlanRequestResult plan_result;
    TPlanRequestParams plan_params;
    plan_params.request_type = TRequestType::Sql;
    plan_params.__set_sql_stmt("select * from tpch.nation");
    plan_params.__set_user(getenv("USER"));
    planner->PlanRequest(plan_result, plan_params);

    EXPECT_EQ(plan_result.schema.cols.size(), 4);
    EXPECT_EQ(plan_result.schema.cols[0].type.type_id, TTypeId::SMALLINT);
    EXPECT_EQ(plan_result.schema.cols[1].type.type_id, TTypeId::STRING);
    EXPECT_EQ(plan_result.schema.cols[2].type.type_id, TTypeId::SMALLINT);
    EXPECT_EQ(plan_result.schema.cols[3].type.type_id, TTypeId::STRING);

    EXPECT_EQ(plan_result.tasks.size(), 1);
    if (plan_result.tasks[0].local_hosts.size() != 3) {
      for (int i = 0; i < plan_result.tasks[0].local_hosts.size(); ++i) {
        const TNetworkAddress& addr = plan_result.tasks[0].local_hosts[i];
        cerr << addr.hostname << ":" << addr.port << endl;
      }
      EXPECT_EQ(plan_result.tasks[0].local_hosts.size(), 3) << "Expecting 3x replication";
    }
    int worker_records = 0;

    TExecTaskResult exec_result;
    TExecTaskParams exec_params;
    exec_params.task = plan_result.tasks[0].task;
    worker->ExecTask(exec_result, exec_params);

    TFetchResult fetch_result;
    TFetchParams fetch_params;
    fetch_params.handle = exec_result.handle;

    do {
      worker->Fetch(fetch_result, fetch_params);
      if (worker_records == 0) {
        // Verify the first row. TODO: verify them all.
        EXPECT_EQ(NATION_TBL[0].c0, *reinterpret_cast<const int16_t*>(
            fetch_result.columnar_records.cols[0].data.data()));

        EXPECT_EQ(NATION_TBL[0].c1.size(), *reinterpret_cast<const int32_t*>(
            fetch_result.columnar_records.cols[1].data.data()));
        EXPECT_EQ(NATION_TBL[0].c1, string(
            fetch_result.columnar_records.cols[1].data.data() + sizeof(int32_t),
            NATION_TBL[0].c1.size()));

        EXPECT_EQ(NATION_TBL[0].c2, *reinterpret_cast<const int16_t*>(
            fetch_result.columnar_records.cols[2].data.data()));

        EXPECT_EQ(NATION_TBL[0].c3.size(), *reinterpret_cast<const int32_t*>(
            fetch_result.columnar_records.cols[3].data.data()));
        EXPECT_EQ(NATION_TBL[0].c3, string(
            fetch_result.columnar_records.cols[3].data.data() + sizeof(int32_t),
            NATION_TBL[0].c3.size()));
      }
      worker_records += fetch_result.num_records;
    } while (!fetch_result.done);

    EXPECT_EQ(worker_records, 25);
    worker->CloseTask(exec_result.handle);
  } catch (const TRecordServiceException& e) {
    EXPECT_TRUE(false) << "Error: " << e.message << " " << e.detail;
  } catch (const TException& e) {
    EXPECT_TRUE(false) << "Error: " << e.what();
  }
}

TEST(ClientTest, NationFile) {
  try {
    shared_ptr<RecordServicePlannerClient> planner =
        CreatePlannerConnection(PLANNER_HOST, RECORD_SERVICE_PLANNER_PORT);

    TPlanRequestResult plan_result;
    TPlanRequestParams plan_params;
    plan_params.request_type = TRequestType::Path;
    plan_params.__isset.path = true;
    plan_params.path.path = "/test-warehouse/tpch.nation/";
    plan_params.__set_user(getenv("USER"));
    planner->PlanRequest(plan_result, plan_params);

    EXPECT_EQ(plan_result.schema.cols.size(), 1);
    EXPECT_EQ(plan_result.schema.cols[0].type.type_id, TTypeId::STRING);
    EXPECT_EQ(plan_result.schema.cols[0].name, "record");

    EXPECT_EQ(plan_result.tasks.size(), 1);
    vector<string> data = FetchAllStrings(plan_result, 0);

    // Verify a few records
    EXPECT_EQ(data[5], "5|ETHIOPIA|0|ven packages wake quickly. regu");
    EXPECT_EQ(data[21], "21|VIETNAM|2|hely enticingly express accounts. even, final ");
  } catch (const TRecordServiceException& e) {
    EXPECT_TRUE(false) << "Error: " << e.message;
  } catch (const TException& e) {
    EXPECT_TRUE(false) << "Error: " << e.what();
  }
}

int main(int argc, char **argv) {
  const char* env = getenv("RUN_MINI_CLUSTER_TESTS");
  if (env != NULL && strcmp(env, "true") == 0) {
    cout << "Skipping non-mini cluster test." << endl;
    return 0;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
