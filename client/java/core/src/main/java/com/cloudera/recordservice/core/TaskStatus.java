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

import java.io.Serializable;
import java.util.List;

import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TTaskStatus;

/**
 * POJO for TTaskStatus.
 */
public class TaskStatus implements Serializable {
  private static final long serialVersionUID = 3726259994166021407L;

  public static final class Stats implements Serializable {
    private static final long serialVersionUID = -5597062895288217097L;

    // See comments for TStats for what these counters mean.
    public final double taskProgress;
    public final long numRecordsRead;
    public final long numRecordsReturned;
    public final long serializeTimeMs;
    public final long clientTimeMs;

    public final boolean hdfsCountersSet;
    public final long decompressTimeMs;
    public final long bytesRead;
    public final long bytesReadLocal;
    public final double hdfsThroughput;

    Stats(TStats stats) {
      taskProgress = stats.task_progress;
      numRecordsRead = stats.num_records_read;
      numRecordsReturned = stats.num_records_returned;
      serializeTimeMs = stats.serialize_time_ms;
      clientTimeMs = stats.client_time_ms;
      decompressTimeMs = stats.decompress_time_ms;
      bytesRead = stats.bytes_read;
      bytesReadLocal = stats.bytes_read_local;
      hdfsThroughput = stats.hdfs_throughput;
      hdfsCountersSet = stats.isSetHdfs_throughput();
    }
  }

  public final Stats stats;
  public final List<LogMessage> dataErrors;
  public final List<LogMessage> warnings;

  TaskStatus(TTaskStatus status) {
    stats = new Stats(status.stats);
    dataErrors = LogMessage.fromThrift(status.data_errors);
    warnings = LogMessage.fromThrift(status.warnings);
  }
}
