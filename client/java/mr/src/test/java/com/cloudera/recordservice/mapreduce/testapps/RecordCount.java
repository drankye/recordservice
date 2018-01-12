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

package com.cloudera.recordservice.mapreduce.testapps;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.cloudera.recordservice.core.TestUtil;

/**
 * MapReduce application that just counts the number of lines in a data set.
 * Can be run with or without record service.
 * TODO: this is a duplciate of what is in the examples package. Fix that.
 */
public class RecordCount {
  public static class Map extends MapReduceBase
      implements Mapper<LongWritable, Text, NullWritable, LongWritable> {
    private final static LongWritable ONE = new LongWritable(1);
    private final static NullWritable NULL = NullWritable.get();

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<NullWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      output.collect(NULL, ONE);
    }
  }

  public static class Reduce extends MapReduceBase
      implements Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
    @Override
    public void reduce(NullWritable key, Iterator<LongWritable> values,
        OutputCollector<NullWritable, LongWritable> output, Reporter reporter)
        throws IOException {
      long sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new LongWritable(sum));
    }
  }

  public static long countRecords(String path) throws IOException {
    String output = TestUtil.getTempDirectory();
    Path inputPath = new Path(path);
    Path outputPath = new Path(output);

    JobConf conf = new JobConf(RecordCount.class);
    conf.setJobName("recordcount");

    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(LongWritable.class);

    conf.setInt("mapreduce.job.reduces", 1);
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(com.cloudera.recordservice.mapred.TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, outputPath);

    JobClient.runJob(conf);

    // Read the result and return it. Since we set the number of reducers to 1,
    // there is always just one file containing the value.
    FileSystem fs = outputPath.getFileSystem(conf);
    FSDataInputStream resultStream = fs.open(new Path(output + "/part-00000"));
    byte[] bytes = new byte[16];
    int length = resultStream.read(bytes);
    String result = new String(bytes, 0, length).trim();
    return Long.parseLong(result);
  }
}
