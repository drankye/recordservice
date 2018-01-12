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

import com.cloudera.recordservice.core.Decimal;

/**
 * Writable for decimal data.
 */
public class DecimalWritable implements WritableComparable<DecimalWritable> {

  private Decimal decimal_;

  public DecimalWritable() {}

  public void set(Decimal decimal) {
    decimal_ = decimal;
  }

  public Decimal get() {
    return decimal_;
  }

  @Override
  // TODO: this assumes a certain usage pattern (the decimal are from the same
  // task). Is this accurate?
  public void write(DataOutput out) throws IOException {
    out.write(decimal_.getBytes());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    in.readFully(decimal_.getBytes(), 0, decimal_.getBytes().length);
  }

  @Override
  public int compareTo(DecimalWritable o) {
    // Comparing the integer values is correct (fixed scale) and cheaper.
    // FIXME: This is not correct if the scale for the decimals is different.
    //assert decimal_.getScale() == o.get().getScale();
    return decimal_.toBigInteger().compareTo(o.get().toBigInteger());
  }
}
