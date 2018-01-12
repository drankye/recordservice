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
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.avro.GenericRecords;
import com.cloudera.recordservice.avro.RecordIterator;
import com.cloudera.recordservice.avro.RecordUtil;
import com.cloudera.recordservice.avro.SchemaUtils;
import com.cloudera.recordservice.avro.SpecificRecords;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase;

/**
 * Input format which provides identical functionality to
 * org.apache.mapreduce.AvroKeyInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AvroKeyInputFormat<T> extends
    RecordServiceInputFormatBase<AvroKey<T>, NullWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroKeyInputFormat.class);

  @Override
  public RecordReader<AvroKey<T>, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Schema readerSchema = AvroJob.getInputKeySchema(context.getConfiguration());
    if (null == readerSchema) {
      LOG.warn("Reader schema was not set. Use AvroJob.setInputKeySchema() if desired.");
      LOG.info("Using a reader schema equal to the writer schema.");
      LOG.info("Using GenericRecords instead of SpecificRecords.");
    }
    return new AvroKeyRecordReader<T>(readerSchema);
  }

  private static class AvroKeyRecordReader<T>
      extends RecordReaderBase<AvroKey<T>, NullWritable> {
    // The schema of the returned records.
    private final Schema avroSchema_;

    // A reusable object to hold the current record.
    private final AvroKey<T> mCurrentRecord;

    // Records to return.
    private RecordIterator<T> records_;

    /**
     * Constructor.
     */
    public AvroKeyRecordReader(Schema schema) {
      avroSchema_ = schema;
      mCurrentRecord = new AvroKey<T>(null);
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      try {
        if (!records_.hasNext()) return false;
        mCurrentRecord.datum(records_.next());
        return true;
      } catch (RecordServiceException e) {
        throw new IOException("Could not fetch record.", e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public AvroKey<T> getCurrentKey() throws IOException, InterruptedException {
      return mCurrentRecord;
    }

    /** {@inheritDoc} */
    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
        throws IOException {
      super.initialize(inputSplit, context);
      if (SchemaUtils.isSpecificRecordSchema(avroSchema_)) {
        records_ = new SpecificRecords(avroSchema_, reader_.records(),
            RecordUtil.ResolveBy.NAME);
      } else {
        records_ = (RecordIterator<T>)new GenericRecords(reader_.records());
      }
    }
  }
}
