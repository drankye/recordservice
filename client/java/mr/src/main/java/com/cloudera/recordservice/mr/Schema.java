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

import org.apache.hadoop.io.Writable;

/**
 * The Schema class provides metadata for records. It is a wrapper for
 * core.Schema but implements the Writable interface.
 */
public class Schema implements Writable {
  private com.cloudera.recordservice.core.Schema schema_;

  public Schema() {
  }

  public Schema(com.cloudera.recordservice.core.Schema schema) {
    schema_ = schema;
  }

  /**
   * Given a column name, returns the column index in the schema using a case-sensitive
   * check on the column name.
   * Returns -1 if the given column name is not found.
   */
  public int getColIdxFromColName(String colName) {
    for (int colIdx = 0; colIdx < schema_.cols.size(); ++colIdx) {
      if (schema_.cols.get(colIdx).name.equals(colName)) {
        return colIdx;
      }
    }
    return -1;
  }

  public com.cloudera.recordservice.core.Schema schema() { return schema_; }
  public int getNumColumns() { return schema_.cols.size(); }
  public com.cloudera.recordservice.core.Schema.ColumnDesc getColumnInfo(int idx) {
    return schema_.cols.get(idx);
  }

  @Override
  public String toString() {
    return schema_.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    schema_ = com.cloudera.recordservice.core.Schema.deserialize(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    schema_.serialize(out);
  }
}
