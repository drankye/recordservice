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

package com.cloudera.recordservice.avro;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;

/**
 * This class takes a Records object and provides an iterator interface to
 * return generic records.
 *
 * TODO: reuse records?
 * TODO: map STRING to BYTES?
 */
public class GenericRecords implements RecordIterator<Record> {
  private Records records_;
  private org.apache.avro.Schema avroSchema_;
  private com.cloudera.recordservice.core.Schema schema_;

  public GenericRecords(Records records) {
    records_ = records  ;
    schema_ = records_.getSchema();
    avroSchema_ = SchemaUtils.convertSchema(schema_);
  }

  /**
   * Returns the generated avro schema.
   */
  @Override
  public org.apache.avro.Schema getSchema() { return avroSchema_; }

  /**
   * Returns true if there are more records, false otherwise.
   */
  @Override
  public boolean hasNext() throws IOException, RecordServiceException {
    return records_.hasNext();
  }

  /**
   * Returns and advances to the next record. Throws exception if
   * there are no more records.
   */
  @Override
  public Record next() throws IOException, RecordServiceException {
    Records.Record rsRecord = records_.next();
    Record record = new Record(avroSchema_);
    for (int i = 0; i < schema_.cols.size(); ++i) {
      if (rsRecord.isNull(i)) {
        record.put(i, null);
        continue;
      }
      switch(schema_.cols.get(i).type.typeId) {
        case BOOLEAN: record.put(i, rsRecord.nextBoolean(i)); break;
        case TINYINT: record.put(i, (int)rsRecord.nextByte(i)); break;
        case SMALLINT: record.put(i, (int)rsRecord.nextShort(i)); break;
        case INT: record.put(i, rsRecord.nextInt(i)); break;
        case BIGINT: record.put(i, rsRecord.nextLong(i)); break;
        case FLOAT: record.put(i, rsRecord.nextFloat(i)); break;
        case DOUBLE: record.put(i, rsRecord.nextDouble(i)); break;
        case STRING:
        case VARCHAR:
        case CHAR:
          record.put(i, rsRecord.nextByteArray(i).toString()); break;
        default:
          throw new RuntimeException(
              "Unsupported type: " + schema_.cols.get(i).type.typeId);
      }
    }
    return record;
  }

  /**
   * Closes the underlying task. Must be called for every GenericRecords object
   * created. Invalid to call other APIs after this. Idempotent.
   */
  @Override
  public void close() {
    records_.close();
  }
}
