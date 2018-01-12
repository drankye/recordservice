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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;

/**
 * This class takes a Rows object and provides an iterator interface to
 * return specific records.
 *
 * TODO: reuse records?
 */
public class SpecificRecords<T extends SpecificRecordBase> implements RecordIterator<T> {
  private Records records_;
  private org.apache.avro.Schema avroSchema_;
  private com.cloudera.recordservice.core.Schema schema_;
  private Class<T> class_;

  // For each field in the RecordService record (by ordinal), the corresponding
  // index in T. If matching by ordinal, this is just the identity. If matching
  // by name, this can be different.
  private int[] recordIndexToRsIndex_;

  @SuppressWarnings("unchecked")
  public SpecificRecords(Schema readerSchema, Records records,
      RecordUtil.ResolveBy resolveBy) {
    avroSchema_ = readerSchema;
    class_ = new SpecificData().getClass(avroSchema_);
    records_ = records;
    schema_ = records_.getSchema();
    recordIndexToRsIndex_ = RecordUtil.resolveSchema(avroSchema_, schema_, resolveBy);
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
  public T next() throws IOException, RecordServiceException {
    T record;

    try {
      record = class_.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Could not create new record instance.", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not create new record instance.", e);
    }

    // add columns for record.
    RecordUtil.buildRecord(
        schema_, records_.next(), avroSchema_, record, recordIndexToRsIndex_);

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
