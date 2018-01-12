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
import java.util.List;

import com.cloudera.recordservice.mr.PlanUtil;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.recordservice.core.ByteArray;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.mr.Schema;

/**
 * Input format that implements the mr TextInputFormat.
 * This only works if the schema of the data is 'STRING'.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TextInputFormat extends
    RecordServiceInputFormatBase<LongWritable, Text> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    PlanUtil.SplitsInfo splits =
        PlanUtil.getSplits(context.getConfiguration(), context.getCredentials());
    verifyTextSchema(splits.schema);
    return splits.splits;
  }

  @Override
  public RecordReader<LongWritable, Text>
      createRecordReader(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
    TextRecordReader rReader = new TextRecordReader();
    rReader.initialize(split, context);
    return rReader;
  }

  /**
   * Verifies that the schema is compatible with TextInputFormat, in particular,
   * that the schema contains a single column. Throws an exception if it is
   * non-matching.
   */
  public static void verifyTextSchema(Schema schema) {
    if (schema.schema().isCountStar) return;
    if (schema.getNumColumns() != 1 ||
        schema.getColumnInfo(0).type.typeId !=
            com.cloudera.recordservice.core.Schema.Type.STRING) {
      throw new RuntimeException(
          "Mismatched schema: TextInputFormat only accepts request that " +
          "return a single STRING column. Schema=" + schema);
    }
  }

  public static class TextRecordReader extends RecordReaderBase<LongWritable, Text> {
    // Value returned for when there is no data. i.e. NULLs and count(*)
    private final static Text EMPTY = new Text();

    // The key corresponding to the record.
    private final LongWritable currentKey_ = new LongWritable();

    // Current value being processed
    private final Text record_ = new Text();

    // The current record number assigned this record. Incremented each time
    // nextKeyValue() is called and assigned to currentKey_.
    private long recordNum_ = 0;

    /**
     * Advances to the next record.
     * Returns true if there are more values to retrieve, false otherwise.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      try {
        if (!reader_.records().hasNext()) return false;
      } catch (RecordServiceException e) {
        // TODO: is this the most proper way to deal with this in MR?
        throw new IOException("Could not fetch record.", e);
      }
      Records.Record record = reader_.records().next();
      if (record.isNull(0)) {
        record_.set(EMPTY);
      } else {
        ByteArray data = record.nextByteArray(0);
        record_.set(data.byteBuffer().array(), data.offset(), data.len());
      }
      currentKey_.set(recordNum_++);
      return true;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return currentKey_;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return record_;
    }
  }
}
