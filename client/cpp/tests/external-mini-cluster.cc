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

#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <sstream>
#include <sys/types.h>
#include <sys/wait.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <vector>

#include "external-mini-cluster.h"
#include "test-common.h"

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace boost;
using namespace std;

namespace recordservice {

const int BASE_PORT = 30000;

const char* ExternalMiniCluster::RecordServiced::NAME = "recordserviced";

ExternalMiniCluster::ExternalMiniCluster(bool debug)
 : debug_(debug),
   impala_home_(getenv("IMPALA_HOME")),
   next_port_(BASE_PORT) {
  if (impala_home_ == NULL) {
    cerr << "Must set IMPALA_HOME" << endl;
    exit(1);
  }
  build_home_ = impala_home_;
  build_home_ += string("/be/build/") + (debug_ ? "debug/" : "release/");
}

ExternalMiniCluster::~ExternalMiniCluster() {
  unordered_set<RecordServiced*> copy = recordserviceds_;
  recordserviceds_.clear();
  for (unordered_set<RecordServiced*>::iterator it = copy.begin(); it != copy.end(); ++it) {
    Kill(*it);
  }
}

string ExternalMiniCluster::RecordServiced::GetBinaryPath() {
  return "service/recordserviced";
}

string ExternalMiniCluster::NextPort() {
  return lexical_cast<string>(next_port_++);
}

bool ExternalMiniCluster::Process::Start() {
  return subprocess_.Start();
}

vector<string> ConstructArgs(const string& binary, const map<string, string>& args) {
  vector<string> ret;
  ret.push_back(binary);

  for (map<string, string>::const_iterator it = args.begin(); it != args.end(); ++it) {
    ret.push_back(string("--") + it->first + "=" + it->second);
  }
  return ret;
}

bool ExternalMiniCluster::StartRecordServiced(
    bool start_record_service_planner, bool start_record_service_worker,
    RecordServiced** process) {
  assert(start_record_service_worker || start_record_service_planner);

  *process = NULL;

  string binary = build_home_ + RecordServiced::GetBinaryPath();
  map<string, string> args;

  args["beeswax_port"] = NextPort();
  args["hs2_port"] = NextPort();
  args["be_port"] = NextPort();
  args["recordservice_webserver_port"] = NextPort();
  args["v"] = "1";

  if (start_record_service_planner) {
    args["recordservice_planner_port"] = NextPort();
  } else {
    args["recordservice_planner_port"] = "0";
  }
  if (start_record_service_worker) {
    args["recordservice_worker_port"] = NextPort();
  } else {
    args["recordservice_worker_port"] = "0";
  }

  *process = new RecordServiced(binary, ConstructArgs(binary, args));
  if (!(*process)->Start()) return false;

  // This while loop waits until the node is up and ready to accept connections
  // TODO: Find a better way to do this
  sleep(1);
  int i = 10;
  while (i > 0) {
    try {
      if (start_record_service_worker) {
        CreateWorkerTransport("localhost",
          atoi(args["recordservice_worker_port"].c_str()));
      } else if (start_record_service_planner) {
        CreatePlannerTransport("localhost",
          atoi(args["recordservice_planner_port"].c_str()));
      }
      break;
    } catch (TTransportException e) {
      sleep(1);
      i = i - 1;
      printf("Sleeping until node is ready...\n");
    }
  }

  if (i == 0) {
    printf("Node did not start. Exiting...\n");
    exit(1);
  }

  if (start_record_service_planner) {
    (*process)->planner_port_ = atoi(args["recordservice_planner_port"].c_str());
  }
  recordserviceds_.insert(*process);
  cout << "Started " << (*process)->name() << " pid: " << (*process)->pid() << endl
       << "    Debug webpage running on port: "
       << args["recordservice_webserver_port"] << endl;
  return true;
}

bool ExternalMiniCluster::Process::Wait(int* ret) {
  return subprocess_.Wait(ret);
}

bool ExternalMiniCluster::Kill(Process* process) {
  cout << "Killing " << process->name() << endl;
  bool ret = process->subprocess_.Kill(SIGKILL);
  if (!ret) {
    cerr << "Could not kill process. ret=" << ret << endl;;
    return false;
  }
  int wait_ret;
  process->Wait(&wait_ret);
  // The process must be a recordserviced
  recordserviceds_.erase((RecordServiced*)process);
  delete process;
  return true;
}

}
