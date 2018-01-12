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

package com.cloudera.recordservice.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.avro.KeyValueRecords;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase;

/**
 * Input format which provides identical functionality to
 * org.apache.mapreduce.AvroKeyValueInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AvroKeyValueInputFormat<K extends SpecificRecordBase,
    V extends SpecificRecordBase> extends
    RecordServiceInputFormatBase<AvroKey<K>, AvroValue<V> > {

  private static final Logger LOG =
      LoggerFactory.getLogger(AvroKeyValueInputFormat.class);

  @Override
  public RecordReader<AvroKey<K>, AvroValue<V>> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Schema keyReaderSchema = AvroJob.getInputKeySchema(context.getConfiguration());
    if (null == keyReaderSchema) {
      LOG.warn("Key reader schema was not set. " +
          "Use AvroJob.setInputKeySchema() if desired.");
      LOG.info("Using a key reader schema equal to the writer schema.");
    }
    Schema valueReaderSchema = AvroJob.getInputValueSchema(context.getConfiguration());
    if (null == valueReaderSchema) {
      LOG.warn("Value reader schema was not set. " +
          "Use AvroJob.setInputValueSchema() if desired.");
      LOG.info("Using a value reader schema equal to the writer schema.");
    }
    return new AvroKeyValueRecordReader<K, V>(keyReaderSchema, valueReaderSchema);
  }

  /**
   * Reads records from the RecordService, returning them as Key,Value pairs.
   * The record returned from the RecordSerive is the union of the key & value schemas.
   *
   * <p>The contents of the 'key' field will be parsed into an AvroKey object.
   * The contents of the 'value' field will be parsed into an AvroValue object.
   * </p>
   *
   * @param <K> The type of the Avro key to read.
   * @param <V> The type of the Avro value to read.
   */
  private static class AvroKeyValueRecordReader<K extends SpecificRecordBase,
      V extends SpecificRecordBase> extends RecordReaderBase<AvroKey<K>, AvroValue<V>> {
    // The schema of the returned records.
    private final Schema keySchema_;
    private final Schema valueSchema_;

    // Records to return.
    private KeyValueRecords<K, V> records_;

    /** The current key the reader is on. */
    private final AvroKey<K> currentKey_;

    /** The current value the reader is on. */
    private final AvroValue<V> currentValue_;

    /**
     * Constructor.
     */
    public AvroKeyValueRecordReader(Schema keySchema, Schema valueSchema) {
      keySchema_ = keySchema;
      valueSchema_ = valueSchema;
      currentKey_ = new AvroKey<K>(null);
      currentValue_ = new AvroValue<V>(null);
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      try {
        if (!records_.hasNext()) return false;
        KeyValueRecords.KeyValuePair<K, V> nextRecord = records_.next();
        currentKey_.datum(nextRecord.KEY);
        currentValue_.datum(nextRecord.VALUE);
        return true;
      } catch (RecordServiceException e) {
        throw new IOException("Could not fetch record.", e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public AvroKey<K> getCurrentKey() throws IOException, InterruptedException {
      return currentKey_;
    }

    /** {@inheritDoc} */
    @Override
    public AvroValue<V> getCurrentValue() throws IOException, InterruptedException {
      return currentValue_;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException {
      super.initialize(inputSplit, context);
      records_ = new KeyValueRecords<K, V>(keySchema_, valueSchema_, reader_.records());
    }
  }
}
