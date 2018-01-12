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

import com.cloudera.recordservice.thrift.TPlanRequestResult;

/**
 * POJO version of TPlanRequestResult
 */
public class PlanRequestResult implements Serializable {
  private static final long serialVersionUID = -1907886354720735451L;
  public final List<Task> tasks;
  public final Schema schema;
  public final UniqueId requestId;
  public final List<NetworkAddress> hosts;
  public final List<LogMessage> warnings;

  public PlanRequestResult(TPlanRequestResult result) {
    tasks = Task.fromThrift(result.tasks);
    schema = new Schema(result.schema);
    requestId = new UniqueId(result.request_id);
    hosts = NetworkAddress.fromThrift(result.hosts);
    warnings = LogMessage.fromThrift(result.warnings);
  }
}
