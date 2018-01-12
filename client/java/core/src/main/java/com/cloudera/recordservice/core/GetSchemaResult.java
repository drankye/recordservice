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

import com.cloudera.recordservice.thrift.TGetSchemaResult;

/**
 * POJO for TGetSchemaResult
 */
public class GetSchemaResult implements Serializable {
  private static final long serialVersionUID = -37431074269811053L;
  public final Schema schema;
  public final List<LogMessage> warnings;

  GetSchemaResult(TGetSchemaResult result) {
    schema = new Schema(result.schema);
    warnings = LogMessage.fromThrift(result.warnings);
  }
}
