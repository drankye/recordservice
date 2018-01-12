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

package com.cloudera.recordservice.mapreduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Records.Record;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import com.cloudera.recordservice.mr.Schema;

/**
 * Input format which returns <NULL, RecordServiceReceord>
 * TODO: is this useful? This introduces the RecordServiceRecord "object"
 * model.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RecordServiceInputFormat extends
    RecordServiceInputFormatBase<NullWritable, RecordServiceRecord> {

  @Override
  public RecordReader<NullWritable, RecordServiceRecord>
      createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    RecordServiceRecordReader rReader = new RecordServiceRecordReader();
    rReader.initialize(split, context);
    return rReader;
  }

  /**
   * RecordReader implementation that uses the RecordService for data access. Values
   * are returned as RecordServiceRecords, which contain schema and data for a single
   * record.
   * To reduce the creation of new objects, existing storage is reused for both
   * keys and values (objects are updated in-place).
   */
  public static class RecordServiceRecordReader extends
      RecordReaderBase<NullWritable, RecordServiceRecord> {
    // Current record being processed
    private Records.Record currentRSRecord_;

    // The record that is returned. Updated in-place when nextKeyValue() is called.
    private RecordServiceRecord record_;

    /**
     * The general contract of the RecordReader is that the client (Mapper) calls
     * this method to load the next Key and Value.. before calling getCurrentKey()
     * and getCurrentValue().
     *
     * Returns true if there are more values to retrieve, false otherwise.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!nextRecord()) return false;
      record_.reset(currentRSRecord_);
      return true;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
      return NullWritable.get();
    }

    @Override
    public RecordServiceRecord getCurrentValue() throws IOException,
        InterruptedException {
      return record_;
    }

    /**
     * Advances to the next record. Return false if there are no more records.
     */
    public boolean nextRecord() throws IOException {
      try {
        if (!reader_.records().hasNext()) return false;
      } catch (RecordServiceException e) {
        // TODO: is this the most proper way to deal with this in MR?
        throw new IOException("Could not fetch record.", e);
      }
      currentRSRecord_ = reader_.records().next();
      return true;
    }

    public Schema schema() { return reader_.schema(); }
    public Record currentRecord() { return currentRSRecord_; }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException {
      super.initialize(split, context);
      record_ = new RecordServiceRecord(schema());
    }
  }
}
