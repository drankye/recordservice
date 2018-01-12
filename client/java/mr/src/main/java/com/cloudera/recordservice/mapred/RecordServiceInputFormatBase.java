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
import java.util.List;

import com.cloudera.recordservice.core.TaskStatus;
import com.cloudera.recordservice.mr.PlanUtil;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.mr.RecordReaderCore;
import com.google.common.base.Preconditions;

public abstract class RecordServiceInputFormatBase<K, V> implements InputFormat<K, V> {
  // Name of record service counters group.
  public final static String COUNTERS_GROUP_NAME = "Record Service Counters";

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    PlanUtil.SplitsInfo splits = PlanUtil.getSplits(job, job.getCredentials());
    return convertSplits(splits.splits);
  }

  /**
   * Converts splits from mapreduce to mapred splits.
   */
  static InputSplit[] convertSplits(
      List<org.apache.hadoop.mapreduce.InputSplit> splits) {
    InputSplit[] retSplits = new InputSplit[splits.size()];
    for (int i = 0; i < splits.size(); ++i) {
      retSplits[i] = new RecordServiceInputSplit(
          (com.cloudera.recordservice.mapreduce.RecordServiceInputSplit) splits.get(i));
    }
    return retSplits;
  }

  /**
   * Populates RecordService counters in ctx from counters.
   */
  public static void setCounters(Reporter ctx, TaskStatus.Stats counters) {
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
  protected abstract static class RecordReaderBase<K,V> implements RecordReader<K, V> {
    private static final Logger LOG =
        LoggerFactory.getLogger(RecordReaderBase.class);

    protected RecordReaderCore reader_;
    protected final Reporter reporter_;

    protected RecordReaderBase(RecordServiceInputSplit split, JobConf config,
        Reporter reporter) throws IOException {
      try {
        reader_ = new RecordReaderCore(config, config.getCredentials(),
            split.getBackingSplit().getTaskInfo());
      } catch (RecordServiceException e) {
        throw new IOException("Failed to execute task.", e);
      }
      reporter_ = reporter;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
      if (reader_ != null) {
        Preconditions.checkNotNull(reporter_);
        try {
          setCounters(reporter_, reader_.records().getStatus().stats);
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
