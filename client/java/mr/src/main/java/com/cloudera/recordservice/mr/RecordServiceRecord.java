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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.cloudera.recordservice.core.ByteArray;
import com.cloudera.recordservice.core.Records.Record;
import com.google.common.base.Preconditions;

public class RecordServiceRecord implements Writable {
  // Array of Writable objects. This is created once and reused.
  private Writable[] columnValObjects_;

  // The values for the current record. If column[i] is NULL,
  // columnVals_[i] is null, otherwise it is columnValObjects_[i].
  private Writable[] columnVals_;

  // Schema associated with columnVals_.
  private Schema schema_;

  // Map of column name to column value index in columnValues_.
  // TODO: Handle column names should be case-insensitive, we need to handle
  // that efficiently.
  private Map<String, Integer> colNameToIdx_;

  public RecordServiceRecord(Schema schema) {
    schema_ = schema;
    columnVals_ = new Writable[schema_.getNumColumns()];
    columnValObjects_ = new Writable[schema_.getNumColumns()];
    colNameToIdx_ = new HashMap<String, Integer>();
    for (int i = 0; i < schema_.getNumColumns(); ++i) {
      colNameToIdx_.put(schema.getColumnInfo(i).name, i);
      columnValObjects_[i] = getWritableInstance(schema.getColumnInfo(i).type.typeId);
    }
  }

  /**
   * Resets the data in this RecordServiceRecord by translating the column data from the
   * given Row to the internal array of Writables (columnVals_).
   * Reads the column data from the given Row into this RecordServiceRecord. The
   * schema are expected to match, minimal error checks are performed.
   * This is a performance critical method.
   */
  public void reset(Record record) {
    if (record.getSchema().cols.size() != schema_.getNumColumns()) {
      throw new IllegalArgumentException(String.format("Schema for new record does " +
        "not match existing schema: %d (new) != %d (existing)",
        record.getSchema().cols.size(), schema_.getNumColumns()));
    }

    for (int i = 0; i < schema_.getNumColumns(); ++i) {
      if (record.isNull(i)) {
        columnVals_[i] = null;
        continue;
      }
      columnVals_[i] = columnValObjects_[i];
      com.cloudera.recordservice.core.Schema.ColumnDesc cInfo = schema_.getColumnInfo(i);
      Preconditions.checkNotNull(cInfo);
      switch (cInfo.type.typeId) {
        case BOOLEAN:
          ((BooleanWritable) columnValObjects_[i]).set(record.nextBoolean(i));
          break;
        case TINYINT:
          ((ByteWritable) columnValObjects_[i]).set(record.nextByte(i));
          break;
        case SMALLINT:
          ((ShortWritable) columnValObjects_[i]).set(record.nextShort(i));
          break;
        case INT:
          ((IntWritable) columnValObjects_[i]).set(record.nextInt(i));
          break;
        case BIGINT:
          ((LongWritable) columnValObjects_[i]).set(record.nextLong(i));
          break;
        case FLOAT:
          ((FloatWritable) columnValObjects_[i]).set(record.nextFloat(i));
          break;
        case DOUBLE:
          ((DoubleWritable) columnValObjects_[i]).set(record.nextDouble(i));
          break;

        case STRING:
        case VARCHAR:
        case CHAR:
          ByteArray s = record.nextByteArray(i);
          ((Text) columnValObjects_[i]).set(
              s.byteBuffer().array(), s.offset(), s.len());
          break;
        case TIMESTAMP_NANOS:
          ((TimestampNanosWritable) columnValObjects_[i]).set(
              record.nextTimestampNanos(i));
          break;
        case DECIMAL:
          ((DecimalWritable) columnValObjects_[i]).set(
              record.nextDecimal(i));
          break;
        default:
          throw new RuntimeException("Unsupported type: " + cInfo);
      }
    }
  }

  public Writable getColumnValue(int colIdx) {
    return columnVals_[colIdx];
  }

  /**
   * Returns the column value for the given column name, or null of the column name
   * does not exist in this record. This is on the hot path when running with Hive -
   * it is called for every record returned.
   */
  public Writable getColumnValue(String columnName) {
    Integer index = colNameToIdx_.get(columnName);
    if (index == null) return null;
    return getColumnValue(index);
  }

  @Override
  // TODO: what is the contract here? Handle NULLs
  public void write(DataOutput out) throws IOException {
    schema_.write(out);
    for(int i = 0; i < columnValObjects_.length; ++i) {
      columnValObjects_[i].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    schema_ = new Schema();
    schema_.readFields(in);
    columnValObjects_ = new Writable[schema_.getNumColumns()];
    for (int i = 0; i < columnValObjects_.length; i++) {
      columnValObjects_[i] = getWritableInstance(schema_.getColumnInfo(i).type.typeId);
      columnValObjects_[i].readFields(in);
    }
  }

  public Schema getSchema() { return schema_; }

  /**
   * Returns the corresponding Writable object for this column type.
   */
  public Writable getWritableInstance(com.cloudera.recordservice.core.Schema.Type type) {
    switch (type) {
      case BOOLEAN: return new BooleanWritable();
      case TINYINT: return new ByteWritable();
      case SMALLINT: return new ShortWritable();
      case INT: return new IntWritable();
      case BIGINT: return new LongWritable();
      case FLOAT: return new FloatWritable();
      case DOUBLE: return new DoubleWritable();
      case VARCHAR:
      case CHAR:
      case STRING: return new Text();
      case TIMESTAMP_NANOS: return new TimestampNanosWritable();
      case DECIMAL: return new DecimalWritable();
      default: throw new UnsupportedOperationException(
          "Unexpected type: " + toString());
    }
  }
}
