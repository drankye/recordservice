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
import java.util.ArrayList;
import java.util.List;

import com.cloudera.recordservice.thrift.TLogMessage;

/**
 * POJO for TLogMessage
 */
public class LogMessage implements Serializable {
  private static final long serialVersionUID = 1677562873174161143L;
  public final String message;
  public final String detail;
  public final LoggingLevel level;
  public final int count;

  LogMessage(TLogMessage msg) {
    message = msg.message;
    detail = msg.detail;
    level = LoggingLevel.fromThrift(msg.level);
    count = msg.count;
  }

  /**
   * Returns a list of LogMessages from the thrift version.
   */
  static List<LogMessage> fromThrift(List<TLogMessage> messages) {
    List<LogMessage> result = new ArrayList<LogMessage>();
    for (TLogMessage m: messages) {
      result.add(new LogMessage(m));
    }
    return result;
  }
}
