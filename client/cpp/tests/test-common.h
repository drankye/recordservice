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

#ifndef RECORD_SERVICE_TEST_COMMON_H
#define RECORD_SERVICE_TEST_COMMON_H

#include <vector>
#include <string>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "gen-cpp/RecordServicePlanner.h"
#include "gen-cpp/RecordServiceWorker.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace recordservice {

inline boost::shared_ptr<TTransport> CreatePlannerTransport(
    const char* planner_host, int planner_port) {
  boost::shared_ptr<TTransport> planner_socket(
      new TSocket(planner_host, planner_port));
  boost::shared_ptr<TTransport> planner_transport(new TBufferedTransport(planner_socket));
  planner_transport->open();
  return planner_transport;
}

inline boost::shared_ptr<TTransport> CreateWorkerTransport(
    const char* worker_host, int worker_port) {
  boost::shared_ptr<TTransport> worker_socket(
      new TSocket(worker_host, worker_port));
  boost::shared_ptr<TTransport> worker_transport(new TBufferedTransport(worker_socket));
  worker_transport->open();
  return worker_transport;
}

inline boost::shared_ptr<RecordServicePlannerClient> CreatePlannerConnection(
    const char* planner_host, int planner_port) {
  boost::shared_ptr<TProtocol> planner_protocol(new TBinaryProtocol(
      CreatePlannerTransport(planner_host, planner_port)));
  boost::shared_ptr<RecordServicePlannerClient> client(
      new RecordServicePlannerClient(planner_protocol));
  return client;
}

inline boost::shared_ptr<RecordServiceWorkerClient> CreateWorkerConnection(
    const char* worker_host, int worker_port) {
  boost::shared_ptr<TProtocol> worker_protocol(new TBinaryProtocol(
      CreateWorkerTransport(worker_host, worker_port)));
  boost::shared_ptr<RecordServiceWorkerClient> worker(
      new RecordServiceWorkerClient(worker_protocol));
  return worker;
}

inline std::vector<std::string> FetchAllStrings(
    TPlanRequestResult& plan_result, int task_idx) {
  EXPECT_GE(task_idx, 0);
  EXPECT_LT(task_idx, plan_result.tasks.size());

  TTask& ttask = plan_result.tasks[task_idx];
  TNetworkAddress address;

  // Find a worker host to connect to
  if (ttask.local_hosts.empty()) {
    EXPECT_FALSE(plan_result.hosts.empty());
    address = plan_result.hosts[0];
  } else {
    address = ttask.local_hosts[0];
  }

  boost::shared_ptr<RecordServiceWorkerClient> worker =
      CreateWorkerConnection(address.hostname.c_str(), address.port);

  TExecTaskResult exec_result;
  TExecTaskParams exec_params;
  exec_params.task = ttask.task;
  worker->ExecTask(exec_result, exec_params);

  TFetchResult fetch_result;
  TFetchParams fetch_params;
  fetch_params.handle = exec_result.handle;

  std::vector<std::string> results;
  do {
    worker->Fetch(fetch_result, fetch_params);
    EXPECT_EQ(fetch_result.columnar_records.cols.size(), 1);
    const char* data = fetch_result.columnar_records.cols[0].data.data();
    for (int i = 0; i < fetch_result.num_records; ++i) {
      int len = *reinterpret_cast<const int*>(data);
      data += sizeof(int);
      results.push_back(std::string(data, len));
      data += len;
    }
  } while (!fetch_result.done);
  worker->CloseTask(exec_result.handle);
  return results;
}

}

#endif
