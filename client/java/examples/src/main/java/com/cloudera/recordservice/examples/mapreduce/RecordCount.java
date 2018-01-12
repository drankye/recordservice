// Copyright 2015 Cloudera Inc.
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

package com.cloudera.recordservice.examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.recordservice.mapreduce.RecordServiceInputFormat;
import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.cloudera.recordservice.mr.RecordServiceRecord;

/**
 * MapReduce application that just counts the number of records in the results
 * returned by the input query.
 */
public class RecordCount extends Configured implements Tool {
  static class Map
      extends Mapper<NullWritable, RecordServiceRecord, NullWritable, LongWritable> {
    private long count = 0;

    @Override
    public void map(NullWritable key, RecordServiceRecord value,
                    Context context) throws IOException {
      ++count;
    }

    @Override
    public void cleanup(Context context)
        throws IOException, InterruptedException {
      context.write(NullWritable.get(), new LongWritable(count));
    }
  }

  static class Reduce
      extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
    @Override
    public void reduce(NullWritable key, Iterable<LongWritable> values,
        Context context) throws IOException, InterruptedException  {
      long sum = 0;
      for (LongWritable v: values) {
        sum += v.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: RecordCount <input_query> <output_path>");
      System.exit(1);
    }
    String inputQuery = args[0];
    String output = args[1];

    Job job = Job.getInstance(getConf());
    job.setJobName("recordcount");
    job.setJarByClass(RecordCount.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(LongWritable.class);

    RecordServiceConfig.setInput(job.getConfiguration(), inputQuery);
    job.setInputFormatClass(RecordServiceInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileSystem fs = FileSystem.get(job.getConfiguration());
    Path outputPath = new Path(output);
    if (fs.exists(outputPath)) fs.delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new RecordCount(), args));
  }
}
