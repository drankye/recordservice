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

package com.cloudera.recordservice.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.Task;

/**
 * Wrapper around core.Task, implementing the Writable interface.
 */
public class TaskInfo implements Writable {

  private Task task_;

  // This is the list of all worker addresses currently available.
  private List<NetworkAddress> workerAddresses_;

  public TaskInfo() {}
  public TaskInfo(Task task, List<NetworkAddress> workerAddresses) {
    task_ = task;
    workerAddresses_ = workerAddresses;
  }

  /**
   * Returns an estimate of the task size. If one task returns a value
   * that is 3x higher, it should take 3x as long. Unit-less.
   */
  public long getLength() { return task_.taskSize; }

  public List<NetworkAddress> getLocations() {
    return task_.localHosts;
  }

  public List<NetworkAddress> getAllWorkerAddresses() {
    return workerAddresses_;
  }

  public Task getTask() { return task_; }

  @Override
  public void write(DataOutput out) throws IOException {
    task_.serialize(out);
    out.writeInt(workerAddresses_.size());
    for (NetworkAddress n: workerAddresses_) {
      out.writeInt(n.hostname.length());
      out.writeBytes(n.hostname);
      out.writeInt(n.port);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    task_ = Task.deserialize(in);
    int numWorkers = in.readInt();
    workerAddresses_ = new ArrayList<NetworkAddress>();
    for (int i = 0; i < numWorkers; ++i) {
      int hostnameLen = in.readInt();
      byte[] hostnameBuffer = new byte[hostnameLen];
      in.readFully(hostnameBuffer);
      int port = in.readInt();
      workerAddresses_.add(new NetworkAddress(new String(hostnameBuffer), port));
    }
  }
}
