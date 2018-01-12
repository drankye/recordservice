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

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.TaskStatus.Stats;
import com.cloudera.recordservice.mr.PlanUtil;
import com.cloudera.recordservice.mr.RecordReaderCore;

/**
 * The base RecordService input format that handles functionality common to
 * all RecordService InputFormats.
 */
public abstract class RecordServiceInputFormatBase<K, V> extends InputFormat<K, V> {
  // Name of record service counters group.
  public final static String COUNTERS_GROUP_NAME = "Record Service Counters";

  private final static Logger LOG =
      LoggerFactory.getLogger(RecordServiceInputFormatBase.class);

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    return PlanUtil.getSplits(
        context.getConfiguration(), context.getCredentials()).splits;
  }

  /**
   * Populates RecordService counters in ctx from counters.
   */
  public static void setCounters(TaskAttemptContext ctx, Stats counters) {
    if (ctx == null) return;
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Read").setValue(
        counters.numRecordsRead);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Records Returned").setValue(
        counters.numRecordsReturned);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Record Serialization Time(ms)").setValue(
        counters.serializeTimeMs);
    ctx.getCounter(COUNTERS_GROUP_NAME, "Client Time(ms)").setValue(
        counters.clientTimeMs);

    if (counters.hdfsCountersSet) {
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read").setValue(
          counters.bytesRead);
      ctx.getCounter(COUNTERS_GROUP_NAME, "Decompression Time(ms)").setValue(
          counters.decompressTimeMs);
      ctx.getCounter(COUNTERS_GROUP_NAME, "Bytes Read Local").setValue(
          counters.bytesReadLocal);
      ctx.getCounter(COUNTERS_GROUP_NAME, "HDFS Throughput(MB/s)").setValue(
          (long)(counters.hdfsThroughput / (1024 * 1024)));
    }
  }

  /**
   * Base class of RecordService based record readers.
   */
  protected abstract static class RecordReaderBase<K,V> extends RecordReader<K, V> {
    private static final Logger LOG =
        LoggerFactory.getLogger(RecordReaderBase.class);
    protected TaskAttemptContext context_;
    protected RecordReaderCore reader_;

    /**
     * Initializes the RecordReader and starts execution of the task.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException {
      RecordServiceInputSplit rsSplit = (RecordServiceInputSplit) split;
      try {
        reader_ = new RecordReaderCore(context.getConfiguration(),
            context.getCredentials(), rsSplit.getTaskInfo());
      } catch (RecordServiceException e) {
        throw new IOException("Failed to execute task.", e);
      }
      context_ = context;
    }

    @Override
    public void close() throws IOException {
      if (reader_ != null) {
        try {
          RecordServiceInputFormatBase.setCounters(
              context_, reader_.records().getStatus().stats);
        } catch (RecordServiceException e) {
          LOG.debug("Could not populate counters: " + e);
        } finally {
          reader_.close();
          reader_ = null;
        }
      }
    }

    @Override
    public float getProgress() throws IOException {
      if (reader_ == null) return 1;
      return reader_.records().progress();
    }
  }
}
