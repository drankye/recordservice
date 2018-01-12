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

package com.cloudera.recordservice.examples.terasort;

import java.io.IOException;
import java.util.List;

import com.cloudera.recordservice.mr.PlanUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.cloudera.recordservice.core.ByteArray;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.Records;
import com.cloudera.recordservice.core.Schema;
import com.cloudera.recordservice.mapreduce.RecordServiceInputFormatBase;

/**
 * Input format to read terasort data. API compatible with TeraInputFormat.
 */
public class RecordServiceTeraInputFormat
    extends RecordServiceInputFormatBase<Text, Text> {

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    PlanUtil.SplitsInfo info = PlanUtil.getSplits(context.getConfiguration(),
        context.getCredentials());
    if (info.schema.getNumColumns() != 1 ||
        info.schema.getColumnInfo(0).type.typeId != Schema.Type.STRING) {
      throw new IOException("Invalid data. Expecting schema to be a single STRING.");
    }
    return info.splits;
  }

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split,
          TaskAttemptContext context) throws IOException, InterruptedException {
    return new TeraSortRecordReader();
  }

  private static final class TeraSortRecordReader extends RecordReaderBase<Text, Text> {
    private Text key = new Text();
    private Text value = new Text();

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      try {
        if (!reader_.records().hasNext()) return false;
      } catch (RecordServiceException e) {
        throw new IOException("Could not fetch records.", e);
      }
      Records.Record record = reader_.records().next();
      assert (!record.isNull(0));
      ByteArray ba = record.nextByteArray(0);
      byte[] array = ba.byteBuffer().array();
      key.set(array, ba.offset(), TeraInputFormat.KEY_LENGTH);
      value.set(array, ba.offset() + TeraInputFormat.KEY_LENGTH,
          TeraInputFormat.VALUE_LENGTH - 1);
      return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }
  }
}
