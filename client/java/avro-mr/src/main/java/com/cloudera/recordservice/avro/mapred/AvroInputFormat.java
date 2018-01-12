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

package com.cloudera.recordservice.avro.mapred;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.recordservice.avro.GenericRecords;
import com.cloudera.recordservice.avro.RecordIterator;
import com.cloudera.recordservice.avro.RecordUtil;
import com.cloudera.recordservice.avro.SchemaUtils;
import com.cloudera.recordservice.avro.SpecificRecords;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.mapred.RecordServiceInputFormatBase;
import com.cloudera.recordservice.mapred.RecordServiceInputSplit;

/**
 * Input format which provides identical functionality to
 * org.apache.mapred.AvroInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AvroInputFormat<T> extends
    RecordServiceInputFormatBase<AvroWrapper<T>, NullWritable> {

  @Override
  public RecordReader<AvroWrapper<T>, NullWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
      throws IOException {
    return new AvroRecordReader<T>(job, reporter, (RecordServiceInputSplit)split);
  }

  /**
   * Implementation of the reader. This just reads records from the RecordService
   * and returns them as Avro objects of type T.
   */
  @SuppressWarnings("unchecked")
  private static class AvroRecordReader<T>
      extends RecordReaderBase<AvroWrapper<T>, NullWritable> {
    private RecordIterator<T> records_;

    @SuppressWarnings("rawtypes")
    public AvroRecordReader(JobConf config, Reporter reporter,
        RecordServiceInputSplit split) throws IOException {
      super(split, config, reporter);
      Schema schema = AvroJob.getInputSchema(config);
      if (SchemaUtils.isSpecificRecordSchema(schema)) {
        records_ = new SpecificRecords(schema, reader_.records(),
            RecordUtil.ResolveBy.NAME);
      } else {
        records_ = (RecordIterator<T>)new GenericRecords(reader_.records());
        AvroJob.setInputSchema(config, records_.getSchema());
      }
    }

    @Override
    public AvroWrapper<T> createKey() {
      return new AvroWrapper<T>(null);
    }

    @Override
    public NullWritable createValue() { return NullWritable.get(); }

    @Override
    public boolean next(AvroWrapper<T> wrapper, NullWritable ignore)
        throws IOException {
      try {
        if (!records_.hasNext()) return false;
        wrapper.datum(records_.next());
        return true;
      } catch (RecordServiceException e) {
        throw new IOException("Could not get next record.", e);
      }
    }
  }
}
