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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.recordservice.examples.User;

import com.cloudera.recordservice.mr.RecordServiceConfig;

public class MapReduceColorCount extends Configured implements Tool {

  public static class ColorCountMapper extends
      Mapper<AvroKey<User>, NullWritable, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text COLOR = new Text();

    @Override
    public void map(AvroKey<User> key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      CharSequence color = key.datum().getFavoriteColor();
      if (color == null) {
        color = "none";
      }
      COLOR.set(color.toString());
      context.write(COLOR, ONE);
    }
  }

  public static class ColorCountReducer extends
      Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
    private final static AvroKey<CharSequence> KEY = new AvroKey<CharSequence>();
    private final static AvroValue<Integer> VALUE = new AvroValue<Integer>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      KEY.datum(key.toString());
      VALUE.datum(sum);
      context.write(KEY, VALUE);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    org.apache.log4j.BasicConfigurator.configure();

    if (args.length != 2) {
      System.err.println("Usage: MapReduceColorCount <input path> <output path>");
      return -1;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(MapReduceColorCount.class);
    job.setJobName("Color Count");

    // RECORDSERVICE:
    // To read from a table instead of a path, comment out
    // FileInputFormat.setInputPaths() and instead use:
    //FileInputFormat.setInputPaths(job, new Path(args[0]));
    RecordServiceConfig.setInputTable(job.getConfiguration(), "rs", "users");

    // RECORDSERVICE:
    // Use the RecordService version of the AvroKeyInputFormat
    job.setInputFormatClass(
        com.cloudera.recordservice.avro.mapreduce.AvroKeyInputFormat.class);
    //job.setInputFormatClass(AvroKeyInputFormat.class);

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(ColorCountMapper.class);
    AvroJob.setInputKeySchema(job, User.getClassSchema());
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    job.setReducerClass(ColorCountReducer.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new MapReduceColorCount(), args);
    System.exit(res);
  }
}
