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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.recordservice.examples.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.recordservice.examples.UserKey;
import com.cloudera.recordservice.examples.UserValue;
import com.cloudera.recordservice.mr.RecordServiceConfig;

/**
 * Count the sum of age for users with same favorite_color.
 */
public class MapReduceAgeCount extends Configured implements Tool {

  public static class AgeCountMapper extends
      Mapper<AvroKey<UserKey>, AvroValue<UserValue>, Text, IntWritable> {
    private static final Text COLOR = new Text();
    private static final IntWritable AGE = new IntWritable();

    @Override
    public void map(AvroKey<UserKey> key, AvroValue<UserValue> value, Context context)
        throws IOException, InterruptedException {
      // The record with Null age is invalid and will not be counted.
      if (value.datum().getAge() != null) {
        AGE.set(value.datum().getAge());
        if (key.datum().getFavoriteColor() == null) {
          COLOR.set("none");
        } else {
          COLOR.set(key.datum().getFavoriteColor().toString());
        }
        context.write(COLOR, AGE);
      }
    }
  }

  public static class AgeCountReducer extends
      Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
    private static final AvroKey<CharSequence> COLOR = new AvroKey<CharSequence>();
    private static final AvroValue<Integer> SUM_OF_AGE = new AvroValue<Integer>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      COLOR.datum(key.toString());
      SUM_OF_AGE.datum(sum);
      context.write(COLOR, SUM_OF_AGE);
    }
  }

  public int run(String[] args) throws Exception {
    org.apache.log4j.BasicConfigurator.configure();

    if (args.length != 2) {
      System.err.println("Usage: MapReduceAgeCount <input path> <output path>");
      return -1;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(MapReduceAgeCount.class);
    job.setJobName("Age Count");

    // RECORDSERVICE:
    // To read from a table instead of a path, comment out
    // FileInputFormat.setInputPaths() and instead use:
    // FileInputFormat.setInputPaths(job, new Path(args[0]));
    RecordServiceConfig.setInputTable(job.getConfiguration(), null, args[0]);

    // RECORDSERVICE:
    // Use the RecordService version of the AvroKeyValueInputFormat
    job.setInputFormatClass(
        com.cloudera.recordservice.avro.mapreduce.AvroKeyValueInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(AgeCountMapper.class);
    // Set schema for input key and value.
    AvroJob.setInputKeySchema(job, UserKey.getClassSchema());
    AvroJob.setInputValueSchema(job, UserValue.getClassSchema());

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    job.setReducerClass(AgeCountReducer.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new MapReduceAgeCount(), args);
    System.exit(res);
  }
}
