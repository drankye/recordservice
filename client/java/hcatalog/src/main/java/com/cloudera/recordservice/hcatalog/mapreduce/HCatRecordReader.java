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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cloudera.recordservice.hcatalog.mapreduce;

import java.io.IOException;

import com.cloudera.recordservice.hcatalog.common.HCatRSUtil;
import com.cloudera.recordservice.mapreduce.RecordServiceInputFormat;
import com.cloudera.recordservice.mapreduce.RecordServiceInputSplit;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HCat wrapper for the underlying RecordReader this ensures that the initialize on
 * the underlying record reader is done with the underlying split not with HCatSplit.
 */
class HCatRecordReader extends RecordReader<WritableComparable, RecordServiceRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HCatRecordReader.class);

  /** The underlying record reader to delegate to. */
  private RecordReader<NullWritable, RecordServiceRecord> baseRecordReader;

  /**
   * Instantiates a new hcat record reader.
   */
  public HCatRecordReader() {
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
   * org.apache.hadoop.mapreduce.InputSplit,
   * org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
                         TaskAttemptContext taskContext)
      throws IOException, InterruptedException {
    RecordServiceInputSplit recordServiceSplit = (RecordServiceInputSplit) split;
    baseRecordReader = createBaseRecordReader(recordServiceSplit, taskContext);
  }

  private RecordReader<NullWritable, RecordServiceRecord> createBaseRecordReader(
      RecordServiceInputSplit recordServiceSplit,
      TaskAttemptContext taskContext) throws IOException {
    JobConf jobConf = HCatUtil.getJobConfFromContext(taskContext);
    HCatRSUtil.copyCredentialsToJobConf(taskContext.getCredentials(), jobConf);
    return new RecordServiceInputFormat().createRecordReader(
        recordServiceSplit, taskContext);
  }

  /* (non-Javadoc)
  * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
  */
  @Override
  public WritableComparable getCurrentKey() throws IOException, InterruptedException {
    return baseRecordReader.getCurrentKey();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   */
  @Override
  public RecordServiceRecord getCurrentValue() throws IOException, InterruptedException {
    return baseRecordReader.getCurrentValue();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() {
    try {
      return baseRecordReader.getProgress();
    } catch (IOException e) {
      LOG.warn("Exception in HCatRecord reader", e);
    }
    catch (InterruptedException e) {
      LOG.warn("Exception in HCatRecord reader", e);
    }
    return 0.0f; // errored
  }

  /**
   * Check if the wrapped RecordReader has another record, and if so convert it into an
   * HCatRecord. We both check for records and convert here so a configurable percent of
   * bad records can be tolerated.
   *
   * @return if there is a next record
   * @throws IOException on error
   * @throws InterruptedException on error
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return baseRecordReader.nextKeyValue();
  }

  /* (non-Javadoc)
  * @see org.apache.hadoop.mapreduce.RecordReader#close()
  */
  @Override
  public void close() throws IOException {
    baseRecordReader.close();
  }
}
