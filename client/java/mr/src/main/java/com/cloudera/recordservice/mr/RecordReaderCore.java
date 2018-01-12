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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServiceWorkerClient;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Task;
import com.cloudera.recordservice.mr.security.DelegationTokenIdentifier;
import com.cloudera.recordservice.mr.security.TokenUtils;

/**
 * Core RecordReader functionality. Classes that implement the MR RecordReader
 * interface should contain this object.
 *
 * This class can authenticate to the worker using delegation tokens. We never
 * try to authenticate using kerberos (the planner should have created the delegation
 * token) to avoid causing issues with the KDC.
 */
public class RecordReaderCore implements Closeable {
  private final static Logger LOG = LoggerFactory.getLogger(RecordReaderCore.class);
  // Underlying worker connection.
  private RecordServiceWorkerClient worker_;

  // Iterator over the records returned by the server.
  private Records records_;

  // Schema for records_
  private Schema schema_;

  // system env for container id
  private static final String CONTAINER_ID = "CONTAINER_ID";

  // Configuration Key for the mapreduce job name
  private static final String JOB_NAME = "mapreduce.job.name";

  /**
   * Creates a RecordReaderCore to read the records for taskInfo.
   */
  @SuppressWarnings("unchecked")
  public RecordReaderCore(Configuration config, Credentials credentials,
      TaskInfo taskInfo) throws RecordServiceException, IOException {
    Token<DelegationTokenIdentifier> token = (Token<DelegationTokenIdentifier>)
        credentials.getToken(DelegationTokenIdentifier.DELEGATION_KIND);
    RecordServiceWorkerClient.Builder builder =
        WorkerUtil.getBuilder(config, TokenUtils.toDelegationToken(token));

    NetworkAddress address = WorkerUtil.getWorkerToConnectTo(
        taskInfo.getTask().taskId, taskInfo.getLocations(),
        taskInfo.getAllWorkerAddresses());
    setTag(taskInfo.getTask(), config);

    try {
      worker_ = builder.connect(address.hostname, address.port);
      records_ = worker_.execAndFetch(taskInfo.getTask());
    } finally {
      if (records_ == null) close();
    }
    schema_ = new Schema(records_.getSchema());
  }

  /**
   * Closes the task and worker connection.
   */
  @Override
  public void close() {
    if (records_ != null) records_.close();
    if (worker_ != null) worker_.close();
  }

  public Records records() { return records_; }
  public Schema schema() { return schema_; }

  /**
   * Set tag for the task.
   */
  private void setTag(Task task, Configuration config) {
    String tag = System.getenv(CONTAINER_ID);
    if (tag == null || tag.isEmpty()) {
      tag = "MR-";
      if (config.get(JOB_NAME) != null) {
        tag += config.get(JOB_NAME);
      }
    }
    task.setTag(tag);
  }
}

