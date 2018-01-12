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

#ifndef RECORD_SERVICE_EXTERNAL_MINI_CLUSTER_H
#define RECORD_SERVICE_EXTERNAL_MINI_CLUSTER_H

#include <unistd.h>
#include <list>
#include <map>
#include <boost/unordered_set.hpp>

#include "subprocess.h"

namespace recordservice {

// Test utility to manage an external (separate process) mini cluster.
// TODO: add SIGSTOP
class ExternalMiniCluster {
 public:
  class Process {
   public:
    virtual const char* name() const = 0;
    pid_t pid() const { return subprocess_.pid(); }

    virtual ~Process() {}

    bool Start();
    bool Wait(int* ret);

    const std::vector<std::string>& GetArgs() { return subprocess_.GetArgs(); }

   protected:
    friend class ExternalMiniCluster;
    Process(const std::string& binary, const std::vector<std::string>& args)
      : subprocess_(binary, args) {
    }

    ExternalMiniCluster* cluster_;
    Subprocess subprocess_;
  };

  class RecordServiced : public Process {
   public:
    virtual const char* name() const { return NAME; }

    // Returns the port the RecordServicePlanner is running on. Returns 0,
    // if not running the planner.
    int recordservice_planner_port() const { return planner_port_; }

   private:
    friend class ExternalMiniCluster;
    RecordServiced(const std::string& binary, const std::vector<std::string>& args)
      : Process(binary, args) {
    }
    static std::string GetBinaryPath();
    static const char* NAME;
    int planner_port_;
  };

  ExternalMiniCluster(bool debug = true);
  ~ExternalMiniCluster();

  // Returns the number of running recordserviceds.
  int num_recordserviceds() const { return recordserviceds_.size(); }

  // Starts an recordservied, optionally running the recordservice planner and worker
  // services.
  bool StartRecordServiced(
      bool start_recordservice_planner, bool start_recordservice_worker,
      RecordServiced** recordserviced);

  // Kills (kill -9) process. The process object is also destroyed after this.
  // Waits until the process is killed before returning.
  bool Kill(Process* process);

  const boost::unordered_set<RecordServiced*>& get_recordserviceds() {
    return recordserviceds_;
  }

 private:
  const bool debug_;
  const char* impala_home_;
  std::string build_home_;
  boost::unordered_set<RecordServiced*> recordserviceds_;

  // TODO: reuse ports for killed processes if we run tests that cycle enough
  // processes.
  int next_port_;

  std::string NextPort();
};

}

#endif
