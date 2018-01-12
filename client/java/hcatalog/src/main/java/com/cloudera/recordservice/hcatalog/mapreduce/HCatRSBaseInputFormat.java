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

import com.cloudera.recordservice.hcatalog.common.HCatRSUtil;
import com.cloudera.recordservice.mr.PlanUtil;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public abstract class HCatRSBaseInputFormat extends
    InputFormat<WritableComparable, RecordServiceRecord> {

  private final static Logger LOG =
          LoggerFactory.getLogger(HCatRSBaseInputFormat.class);

  // TODO: needs to go in InitializeInput? as part of InputJobInfo
  private static HCatSchema getOutputSchema(Configuration conf) throws IOException {
    String os = conf.get(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA);
    if (os == null) {
      return getTableSchema(conf);
    } else {
      return (HCatSchema) HCatUtil.deserialize(os);
    }
  }

  /**
   * Set the schema for the HCatRecord data returned by HCatInputFormat.
   * @param job the job object
   * @param hcatSchema the schema to use as the consolidated schema
   */
  public static void setOutputSchema(Job job, HCatSchema hcatSchema)
    throws IOException {
    job.getConfiguration().set(HCatConstants.HCAT_KEY_OUTPUT_SCHEMA,
      HCatUtil.serialize(hcatSchema));
  }

  /**
   * Logically split the set of input files for the job. Returns the
   * underlying InputFormat's splits
   * @param jobContext the job context object
   * @return the splits, a RecordServiceSplit wrapper over the storage
   *         handler InputSplits
   * @throws IOException or InterruptedException
   */
  @Override
  public List<InputSplit> getSplits(JobContext jobContext)
    throws IOException, InterruptedException {
    Configuration conf = jobContext.getConfiguration();
    // Get the job info from the configuration,
    // throws exception if not initialized
    InputJobInfo inputJobInfo;
    try {
      inputJobInfo = getJobInfo(conf);
    } catch (Exception e) {
      throw new IOException(e);
    }

    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<PartInfo> partitionInfoList = inputJobInfo.getPartitions();
    if (partitionInfoList == null) {
      // No partitions match the specified partition filter
      return splits;
    }

    JobConf jobConf = HCatUtil.getJobConfFromContext(jobContext);
    Credentials credentials = jobContext.getCredentials();
    PlanUtil.SplitsInfo splitsInfo = PlanUtil.getSplits(jobConf, credentials);
    return splitsInfo.splits;
  }

  /**
   * Create the RecordReader for the given InputSplit. Returns the underlying
   * RecordReader if the required operations are supported and schema matches
   * with HCatTable schema. Returns an HCatRecordReader if operations need to
   * be implemented in HCat.
   * @param split the split
   * @param taskContext the task attempt context
   * @return the record reader instance, either an HCatRecordReader(later) or
   *         the underlying storage handler's RecordReader
   * @throws IOException or InterruptedException
   */
  @Override
  public RecordReader<WritableComparable, RecordServiceRecord>
  createRecordReader(InputSplit split,
             TaskAttemptContext taskContext) throws IOException, InterruptedException {
    JobConf jobConf = HCatUtil.getJobConfFromContext(taskContext);
    HCatRSUtil.copyCredentialsToJobConf(taskContext.getCredentials(), jobConf);
    return new HCatRecordReader();
  }


  /**
   * Gets the HCatTable schema for the table specified in the HCatInputFormat.setInput
   * call on the specified job context. This information is available only after
   * HCatInputFormat.setInput has been called for a JobContext.
   * @param conf the Configuration object
   * @return the table schema
   * @throws IOException if HCatInputFormat.setInput has not been called
   *                     for the current context
   */
  public static HCatSchema getTableSchema(Configuration conf)
    throws IOException {
    InputJobInfo inputJobInfo = getJobInfo(conf);
    HCatSchema allCols = new HCatSchema(new LinkedList<HCatFieldSchema>());
    for (HCatFieldSchema field :
      inputJobInfo.getTableInfo().getDataColumns().getFields()) {
      allCols.append(field);
    }
    for (HCatFieldSchema field :
      inputJobInfo.getTableInfo().getPartitionColumns().getFields()) {
      allCols.append(field);
    }
    return allCols;
  }

  /**
   * Gets the InputJobInfo object by reading the Configuration and deserializing
   * the string. If InputJobInfo is not present in the configuration, throws an
   * exception since that means HCatInputFormat.setInput has not been called.
   * @param conf the Configuration object
   * @return the InputJobInfo object
   * @throws IOException the exception
   */
  private static InputJobInfo getJobInfo(Configuration conf)
    throws IOException {
    String jobString = conf.get(HCatConstants.HCAT_KEY_JOB_INFO);
    if (jobString == null) {
      throw new IOException("job information not found in JobContext."
        + " HCatInputFormat.setInput() not called?");
    }

    return (InputJobInfo) HCatRSUtil.deserialize(jobString);
  }

}
