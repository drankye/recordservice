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

import java.sql.Timestamp;

/**
 * Conversion from encoded timestamp from the serialized format to other
 * formats. This class does lazy conversion and is intended to be reused
 * across values.
 * Values are stored as time since EPOCH 1970-01-01 GMT.
 */
public class TimestampNanos {
  // Milliseconds since epoch.
  private long millis_;
  // Nano second offset.
  private int nanos_;

  // Timestamp object to return. Reused.
  private Timestamp timestamp_;

  public TimestampNanos() {
    timestamp_ = new Timestamp(0);
  }

  public void set(long millis, int nanos) {
    millis_ = millis;
    nanos_ = nanos;
  }

  /**
   * Convert to a sql Timestamp object (always in GMT)
   */
  public Timestamp toTimeStamp() {
    timestamp_.setTime(millis_);
    timestamp_.setNanos(nanos_);
    return timestamp_;
  }

  public int getNanos() { return nanos_; }
  public long getMillisSinceEpoch() { return millis_; }
}
