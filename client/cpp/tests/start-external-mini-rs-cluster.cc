// Copyright 2015 Cloudera Inc.
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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <gtest/gtest.h>
#include <jni.h>
#include <vector>
#include <string>

#include "external-mini-cluster.h"
#include "test-common.h"
#include "subprocess.h"

using namespace boost;
using namespace std;

namespace recordservice {

ExternalMiniCluster cluster;

void ExitIfFalse(bool value) {
  if (!value) {
    printf("Hit boolean assertion error. Exiting...\n");
    exit(1);
  }
}

// This method pipes a command to the system and returns the result as a string
string ExecuteCmdOnMachine(const char* cmd) {
  FILE* pipe = popen(cmd, "r");
  if (!pipe) {
    printf("%s%s\n", "Error executing cmd: ", cmd);
    exit(1);
  }
  char buffer[128];
  string result = "";
  while (!feof(pipe)) {
    if (fgets(buffer, 128, pipe) != NULL) {
      result += buffer;
    }
  }
  pclose(pipe);
  return result;
}

vector<int> GetRunningRecordServicedPids() {
  const unordered_set<ExternalMiniCluster::RecordServiced*>& recordserviceds =
      cluster.get_recordserviceds();
  vector<int> pids;
  for (unordered_set<ExternalMiniCluster::RecordServiced*>::iterator it =
      recordserviceds.begin();
    it != recordserviceds.end(); ++it) {
    pids.push_back((*it)->pid());
  }
  return pids;
}

int GetSpecificNodePid(int planner_port) {
  const unordered_set<ExternalMiniCluster::RecordServiced*>& recordserviceds =
      cluster.get_recordserviceds();
  for (unordered_set<ExternalMiniCluster::RecordServiced*>::iterator it =
      recordserviceds.begin();
    it != recordserviceds.end(); ++it) {
    if ((*it)->recordservice_planner_port() == planner_port) {
      return (*it)->pid();
    }
  }
  return -1;
}

ExternalMiniCluster::Process* GetRecordServicedByPid(int pid) {
  const unordered_set<ExternalMiniCluster::RecordServiced*>& recordserviceds =
      cluster.get_recordserviceds();
  for (unordered_set<ExternalMiniCluster::RecordServiced*>::iterator it =
      recordserviceds.begin();
    it != recordserviceds.end(); ++it) {
    if ((*it)->pid() == pid) {
      return *it;
    }
  }
  return NULL;
}

void KillNodeByPid(int pid) {
  ExternalMiniCluster::Process* node = GetRecordServicedByPid(pid);
  if (node != NULL) {
    cluster.Kill(node);
  } else {
    printf("KillNodeByPid: node %d not found.", pid);
  }
}

int AddRecordServiceNode(bool start_planner, bool start_worker) {
  ExternalMiniCluster::RecordServiced* recordserviced;
  bool result = cluster.StartRecordServiced(start_planner, start_worker, &recordserviced);
  ExitIfFalse(result);
  ExitIfFalse(recordserviced != NULL);
  return recordserviced->pid();
}

// This method starts a mini cluster with a specified number of nodes, all of them
// running both as planner and worker. This method does not return.
void StartMiniCluster(int num_nodes) {
  ExternalMiniCluster::RecordServiced* recordservice_planner = NULL;
  bool result = false;
  for (int i = 0; i < num_nodes; ++i) {
    ExternalMiniCluster::RecordServiced* recordserviced;
    result = cluster.StartRecordServiced(true, true, &recordserviced);
    ExitIfFalse(result);
    ExitIfFalse(recordserviced != NULL);
    if (recordservice_planner == NULL) recordservice_planner = recordserviced;
  }
  while (1) {
    sleep(10);
  }
}

}

extern "C"
JNIEXPORT void JNICALL
Java_com_cloudera_recordservice_tests_MiniClusterController_StartMiniCluster(
    JNIEnv* env, jclass caller_class, jint num_nodes) {
  recordservice::StartMiniCluster(num_nodes);
}

extern "C"
JNIEXPORT jobjectArray JNICALL
Java_com_cloudera_recordservice_tests_MiniClusterController_GetNodeArgs(
    JNIEnv* env, jclass caller_class, jint pid) {
  recordservice::ExternalMiniCluster::Process* node =
      recordservice::GetRecordServicedByPid(pid);
  if (node == NULL) {
    return NULL;
  }
  const vector<string>& args = node->GetArgs();
  jobjectArray result = env->NewObjectArray(args.size(),
      env->FindClass("java/lang/String"), NULL);
  for(int i = 0; i < args.size(); i++) {
    env->SetObjectArrayElement(result, i, env->NewStringUTF(args[i].c_str()));
  }
  return result;
}

extern "C"
JNIEXPORT void JNICALL
Java_com_cloudera_recordservice_tests_MiniClusterController_KillNodeByPid(
    JNIEnv* env, jclass caller_class, jint pid) {
  recordservice::KillNodeByPid((int) pid);
}

extern "C"
JNIEXPORT jint JNICALL
Java_com_cloudera_recordservice_tests_MiniClusterController_AddRecordServiceNode(
    JNIEnv* env, jclass caller_class, jboolean start_planner, jboolean start_worker) {
  return recordservice::AddRecordServiceNode(
      (bool) start_planner, (bool) start_worker);
}

extern "C"
JNIEXPORT jintArray JNICALL
Java_com_cloudera_recordservice_tests_MiniClusterController_GetRunningMiniNodePids(
    JNIEnv* env, jclass caller_class) {
  vector<int> pid_vector = recordservice::GetRunningRecordServicedPids();
  jintArray result = (env)->NewIntArray(pid_vector.size());
  env->SetIntArrayRegion(result, 0, pid_vector.size(), &pid_vector[0]);
  return result;
}

extern "C"
JNIEXPORT jint JNICALL
Java_com_cloudera_recordservice_tests_MiniClusterController_GetSpecificNodePid(
    JNIEnv* env, jclass caller_class, jint planner_port) {
  return recordservice::GetSpecificNodePid(planner_port);
}

// The main method starts up a mini cluster. As the cluster shuts down when the mini
// cluster object goes out of scope, this main method never returns.
int main(int argc, char **argv) {
  recordservice::StartMiniCluster(3);
}
