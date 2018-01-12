/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.recordservice.avro;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;

/**
 * This class takes a Records object and provides an iterator interface to
 * return key / value records.
 */
public class KeyValueRecords<K extends SpecificRecordBase, V extends SpecificRecordBase>
    implements Closeable {
  private Records records_;
  private org.apache.avro.Schema keySchema_;
  private org.apache.avro.Schema valueSchema_;
  private com.cloudera.recordservice.core.Schema schema_;
  private Class<K> keyClass_;
  private Class<V> valueClass_;

  // The key / value index mapping to rsIndex.
  private int[] keyIndexToRsIndex_;
  private int[] valueIndexToRsIndex_;

  @SuppressWarnings("unchecked")
  public KeyValueRecords(Schema keySchema, Schema valueSchema, Records records) {
    keySchema_ = keySchema;
    valueSchema_ = valueSchema;
    keyClass_ = new SpecificData().getClass(keySchema_);
    valueClass_ = new SpecificData().getClass(valueSchema_);
    records_ = records;
    schema_ = records_.getSchema();
    // Verifies and resolves the schema of Key / value with the RecordService schema.
    keyIndexToRsIndex_ = RecordUtil.resolveSchema(keySchema_, schema_,
        RecordUtil.ResolveBy.NAME);
    valueIndexToRsIndex_ = RecordUtil.resolveSchema(valueSchema_, schema_,
        RecordUtil.ResolveBy.NAME);
  }

  /**
   * Provide a record with key / value.
   */
  public static class KeyValuePair<K, V> {
    public final K KEY;
    public final V VALUE;

    public KeyValuePair(K key, V value) {
      KEY = key;
      VALUE = value;
    }
  }

  /**
   * Returns true if there are more records, false otherwise.
   */
  public boolean hasNext() throws IOException, RecordServiceException {
    return records_.hasNext();
  }

  /**
   * Returns the next key / value record. Throws exception if there are no more records.
   */
  public KeyValuePair<K, V> next() throws IOException, RecordServiceException {
    K key;
    V value;

    try {
      key = keyClass_.newInstance();
      value = valueClass_.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("Could not create new key/value record instance.", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not create new key/value record instance.", e);
    }

    Records.Record rsRecord = records_.next();

    // Add columns for the key record.
    RecordUtil.buildRecord(schema_, rsRecord, keySchema_, key, keyIndexToRsIndex_);

    // Add columns for the value record.
    RecordUtil.buildRecord(schema_, rsRecord, valueSchema_, value, valueIndexToRsIndex_);

    return new KeyValuePair<K, V>(key, value);
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
