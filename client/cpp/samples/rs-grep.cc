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

#include <stdio.h>
#include <exception>
#include <iostream>
#include <sstream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <gflags/gflags.h>

#include "gen-cpp/RecordServicePlanner.h"
#include "gen-cpp/RecordServiceWorker.h"

using namespace boost;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace recordservice;

DEFINE_string(planner_host, "localhost", "The host running the planner service.");
DEFINE_int32(planner_port, 12050, "The port of the planner service.");

// Implementation of some standard grep options.
DEFINE_bool(invert_match, false, "select non-matching lines");
DEFINE_bool(line_regexp, false, "select only matches that match the whole line");
DEFINE_int64(max_count, -1, "maximum number of matches to return");

static int64_t records_returned = 0;

shared_ptr<RecordServicePlannerClient> CreatePlannerConnection() {
  shared_ptr<TTransport> planner_socket(
      new TSocket(FLAGS_planner_host, FLAGS_planner_port));
  shared_ptr<TTransport> planner_transport(new TBufferedTransport(planner_socket));
  shared_ptr<TProtocol> planner_protocol(new TBinaryProtocol(planner_transport));
  shared_ptr<RecordServicePlannerClient> client(
      new RecordServicePlannerClient(planner_protocol));
  planner_transport->open();
  return client;
}

shared_ptr<RecordServiceWorkerClient> CreateWorkerConnection(
    const TNetworkAddress& host) {
  shared_ptr<TTransport> worker_socket(new TSocket(host.hostname, host.port));
  shared_ptr<TTransport> worker_transport(new TBufferedTransport(worker_socket));
  shared_ptr<TProtocol> worker_protocol(new TBinaryProtocol(worker_transport));
  worker_transport->open();
  shared_ptr<RecordServiceWorkerClient> worker(
      new RecordServiceWorkerClient(worker_protocol));
  return worker;
}

void ProcessTask(const TTask& task) {
  shared_ptr<RecordServiceWorkerClient> worker =
      CreateWorkerConnection(task.local_hosts[0]);

  TExecTaskResult exec_result;
  TExecTaskParams exec_params;
  exec_params.task = task.task;
  worker->ExecTask(exec_result, exec_params);

  TFetchResult fetch_result;
  TFetchParams fetch_params;
  fetch_params.handle = exec_result.handle;

  do {
    worker->Fetch(fetch_result, fetch_params);
    const char* data = fetch_result.columnar_records.cols[0].data.data();
    for (int i = 0; i < fetch_result.num_records; ++i) {
      int len = *reinterpret_cast<const int*>(data);
      data += sizeof(int);
      size_t r = write(0, data, len);
      if (r != len) throw TException("Could not output result");
      r = write(0, "\n", 1);
      if (r != 1) throw TException("Could not output result");
      data += len;
      ++records_returned;
      if (records_returned == FLAGS_max_count) goto end;
    }
  } while (!fetch_result.done);

end:
  worker->CloseTask(exec_result.handle);
}

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (argc != 3) {
    printf("Usage: rs_grep [OPTION]... PATTERN [DIR]\n");
    return 1;
  }

  // Map the arguments to a SQL query.
  stringstream query;
  query << "SELECT * FROM __PATH__ WHERE record ";
  if (FLAGS_invert_match) query << "NOT ";
  if (FLAGS_line_regexp) {
    query << "LIKE \"" + string(argv[1]) + "\"";
  } else {
    query << "LIKE \"\%" + string(argv[1]) + "\%\"";
  }
  if (FLAGS_max_count >= 0) query << " LIMIT " << FLAGS_max_count;

  try {
    shared_ptr<RecordServicePlannerClient> planner = CreatePlannerConnection();

    TPlanRequestResult plan_result;
    TPlanRequestParams plan_params;
    plan_params.request_type = TRequestType::Path;
    plan_params.__isset.path = true;
    plan_params.path.path = argv[2];
    plan_params.path.__set_query(query.str());
    planner->PlanRequest(plan_result, plan_params);

    if (plan_result.tasks.size() == 0) {
      cerr << "rs-grep: " << argv[2] << ": No such file or directory" << endl;
    }

    for (int i = 0; i < plan_result.tasks.size(); ++i) {
      ProcessTask(plan_result.tasks[i]);
      if (records_returned == FLAGS_max_count) break;
    }
  } catch (const TRecordServiceException& e) {
    cerr << "rs-grep: " << e.message << endl;
    cerr << "detail: " << e.detail;
  } catch (const TException& e) {
    cerr << "rs-grep: " << e.what() << endl;
  }

  return 0;
}
