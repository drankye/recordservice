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

/**
 * POJO for LoggingLevel
 */
public enum LoggingLevel implements Serializable {
  OFF,
  ALL,
  FATAL,
  ERROR,
  WARN,
  INFO,
  DEBUG,
  TRACE;

  com.cloudera.recordservice.thrift.TLoggingLevel toThrift() {
    switch (this) {
      case OFF: return com.cloudera.recordservice.thrift.TLoggingLevel.OFF;
      case ALL: return com.cloudera.recordservice.thrift.TLoggingLevel.ALL;
      case FATAL: return com.cloudera.recordservice.thrift.TLoggingLevel.FATAL;
      case ERROR: return com.cloudera.recordservice.thrift.TLoggingLevel.ERROR;
      case WARN: return com.cloudera.recordservice.thrift.TLoggingLevel.WARN;
      case INFO: return com.cloudera.recordservice.thrift.TLoggingLevel.INFO;
      case DEBUG: return com.cloudera.recordservice.thrift.TLoggingLevel.DEBUG;
      case TRACE: return com.cloudera.recordservice.thrift.TLoggingLevel.TRACE;
    }
    throw new RuntimeException("Unrecognized logging level: " + this);
  }

  static LoggingLevel fromThrift(com.cloudera.recordservice.thrift.TLoggingLevel l) {
    switch (l) {
      case OFF: return OFF;
      case ALL: return ALL;
      case FATAL: return FATAL;
      case ERROR: return ERROR;
      case WARN: return WARN;
      case INFO: return INFO;
      case DEBUG: return DEBUG;
      case TRACE: return TRACE;
    }
    throw new RuntimeException("Unrecognized logging level: " + l);
  }
}
