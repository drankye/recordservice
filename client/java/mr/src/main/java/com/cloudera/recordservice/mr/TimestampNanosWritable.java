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

import org.apache.hadoop.io.WritableComparable;

import com.cloudera.recordservice.core.TimestampNanos;

/**
 * Writable for timestamp data with nanosecond precision.
 */
public class TimestampNanosWritable
    implements WritableComparable<TimestampNanosWritable> {

  TimestampNanos ts_ = new TimestampNanos();

  public TimestampNanosWritable() {}
  public TimestampNanosWritable(TimestampNanos ts) {
    set(ts);
  }

  public void set(TimestampNanos ts) {
    ts_ = ts;
  }

  public void set(long millis, int nanos) {
    ts_.set(millis, nanos);
  }

  public TimestampNanos get() {
    return ts_;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(ts_.getMillisSinceEpoch());
    out.writeInt(ts_.getNanos());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    set(in.readLong(), in.readInt());
  }

  @Override
  public int compareTo(TimestampNanosWritable o) {
    if (ts_.getMillisSinceEpoch() < o.ts_.getMillisSinceEpoch()) {
      return -1;
    }
    if (ts_.getMillisSinceEpoch() > o.ts_.getMillisSinceEpoch()) {
      return 1;
    }
    return ts_.getNanos() - o.ts_.getNanos();
  }
}
