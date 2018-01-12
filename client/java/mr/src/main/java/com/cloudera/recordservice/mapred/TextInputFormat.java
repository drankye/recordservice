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

package com.cloudera.recordservice.mapred;

import java.io.IOException;

import com.cloudera.recordservice.mr.PlanUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.recordservice.core.ByteArray;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;

/**
 * Input format that implements the mr TextInputFormat.
 * This only works if the schema of the data is 'STRING'.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TextInputFormat extends RecordServiceInputFormatBase<LongWritable, Text> {
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    PlanUtil.SplitsInfo splits = PlanUtil.getSplits(job, job.getCredentials());
    com.cloudera.recordservice.mapreduce.TextInputFormat.verifyTextSchema(splits.schema);
    return convertSplits(splits.splits);
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new TextRecordReader((RecordServiceInputSplit)split, job, reporter);
  }

  private static final class TextRecordReader
      extends RecordReaderBase<LongWritable, Text> {
    // Value returned for when there is no data. i.e. NULLs and count(*)
    private final static Text EMPTY = new Text();

    private long recordNum_ = 0;

    public TextRecordReader(RecordServiceInputSplit split, JobConf config,
        Reporter reporter) throws IOException {
      super(split, config, reporter);
      com.cloudera.recordservice.mapreduce.TextInputFormat.verifyTextSchema(
          split.getBackingSplit().getSchema());
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      try {
        if (!reader_.records().hasNext()) return false;
        Records.Record record = reader_.records().next();
        if (record.isNull(0)) {
          value.set(EMPTY);
        } else {
          ByteArray data = record.nextByteArray(0);
          value.set(data.byteBuffer().array(), data.offset(), data.len());
        }
        key.set(recordNum_++);
        return true;
      } catch (RecordServiceException e) {
        throw new IOException("Could not get next record.", e);
      }
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }

    @Override
    public Text createValue() {
      return new Text();
    }
  }
}
