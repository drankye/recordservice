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

package com.cloudera.recordservice.avro;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class which reads the boolean config:
 *   "recordservice.enable.avro"
 * and automatically switches non-recordserivce input formats to their recordservice
 * equivalents.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AvroJob {
  public final static String USE_RECORD_SERVICE_INPUT_FORMAT_CONF_KEY =
      "recordservice.enable.avro";

  private static final Logger LOG = LoggerFactory.getLogger(AvroJob.class);

  public static void setInputFormatClass(org.apache.hadoop.mapreduce.Job job,
      Class<? extends org.apache.hadoop.mapreduce.InputFormat> c) {
    if (job.getConfiguration().getBoolean(
        USE_RECORD_SERVICE_INPUT_FORMAT_CONF_KEY, false)) {
      if (c.getName().equals(
          org.apache.avro.mapreduce.AvroKeyInputFormat.class.getName())) {
        c = com.cloudera.recordservice.avro.mapreduce.AvroKeyInputFormat.class;
      } else if (c.getName().equals(
          org.apache.avro.mapreduce.AvroKeyValueInputFormat.class.getName())) {
        c = com.cloudera.recordservice.avro.mapreduce.AvroKeyValueInputFormat.class;
      } else {
        throw new RuntimeException("Class '" + c.getName() + "' is not supported by " +
            "the RecordService. Use AvroKeyValueInputFormat or " +
            "AvroKeyInputFormat or disable RecordService.");
      }
    }
    LOG.debug("Using input format: " + c.getName());
    job.setInputFormatClass(c);
  }

  public static void setInputFormat(org.apache.hadoop.mapred.JobConf job,
      Class<? extends org.apache.hadoop.mapred.InputFormat> c) {
    if (job.getBoolean(USE_RECORD_SERVICE_INPUT_FORMAT_CONF_KEY, false)) {
      if (c.getName().equals(org.apache.avro.mapred.AvroInputFormat.class.getName())) {
        c = com.cloudera.recordservice.avro.mapred.AvroInputFormat.class;
      } else {
        throw new RuntimeException("Class '" + c.getName() + "' is not supported "
            + "by the RecordService. Use AvroInputFormat or disable RecordService.");
      }
    }
    LOG.debug("Using input format: " + c.getName());
    job.setInputFormat(c);
  }
}
