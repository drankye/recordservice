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

package com.cloudera.recordservice.core;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WorkerClientUtil {
  /**
   * Utility function to launch task 'taskId' in plan at the first host.
   * Local hosts are preferred over remote hosts.
   *
   * TODO: move this into a test/sample directory? Real frameworks have
   * to deal with much more stuff.
   */
  public static Records execTask(PlanRequestResult plan, int taskId)
      throws RecordServiceException, IOException {
    if (taskId >= plan.tasks.size() || taskId < 0) {
      throw new RuntimeException("Invalid task id.");
    }

    Task task = plan.tasks.get(taskId);
    NetworkAddress host;
    if (task.localHosts.isEmpty()) {
      if (plan.hosts.isEmpty()) {
        throw new RuntimeException("No hosts are provided to run this task.");
      }
      host = plan.hosts.get(0);
    } else {
      host = task.localHosts.get(0);
    }

    Records records = new RecordServiceWorkerClient.Builder()
        .connect(host.hostname, host.port).execAndFetch(task);
    records.setCloseWorker(true);
    return records;
  }
}
