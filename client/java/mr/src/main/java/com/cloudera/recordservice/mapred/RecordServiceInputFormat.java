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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.mr.RecordServiceRecord;

/**
 * Input format which returns (NULL, RecordServiceRecord).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RecordServiceInputFormat extends
    RecordServiceInputFormatBase<WritableComparable<?>, RecordServiceRecord> {

  @Override
  public RecordReader<WritableComparable<?>, RecordServiceRecord>
      getRecordReader(InputSplit split, JobConf job, Reporter reporter)
          throws IOException {
    return new RecordServiceRecordReader((RecordServiceInputSplit)split, job, reporter);
  }

  public static class RecordServiceRecordReader
      extends RecordReaderBase<WritableComparable<?>, RecordServiceRecord> {
    public RecordServiceRecordReader(RecordServiceInputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      super(split, job, reporter);
    }

    /**
     * Advances to the next record, populating key,value with the results.
     * This is the hot path.
     */
    @Override
    public boolean next(WritableComparable<?> key, RecordServiceRecord value)
        throws IOException {
      try {
        if (!reader_.records().hasNext()) return false;
      } catch (RecordServiceException e) {
        // TODO: is this the most proper way to deal with this in MR?
        throw new IOException("Could not fetch record.", e);
      }
      value.reset(reader_.records().next());
      return true;
    }

    @Override
    public WritableComparable<?> createKey() {
      return NullWritable.get();
    }

    @Override
    public RecordServiceRecord createValue() {
      return new RecordServiceRecord(reader_.schema());
    }
  }
}
