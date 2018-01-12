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

import com.cloudera.recordservice.thrift.TColumnarRecords;
import com.cloudera.recordservice.thrift.TFetchResult;

/**
 * POJO for TFetchResult
 */
public class FetchResult implements Serializable {
  private static final long serialVersionUID = -431738547391113814L;

  public enum RecordFormat implements Serializable {
    Columnar,
  }

  public final boolean done;
  public final double taskProgress;
  public final int numRecords;
  public final RecordFormat recordFormat;

  final TColumnarRecords records;

  FetchResult(TFetchResult result) {
    done = result.done;
    taskProgress = result.task_progress;
    numRecords = result.num_records;
    records = result.columnar_records;
    switch (result.record_format) {
      case Columnar:
        recordFormat = RecordFormat.Columnar;
        break;
      default:
        throw new RuntimeException("Unknown record format: " + result.record_format);
    }
  }
}
