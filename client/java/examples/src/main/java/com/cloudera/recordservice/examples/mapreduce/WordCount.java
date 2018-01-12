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

package com.cloudera.recordservice.examples.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.cloudera.recordservice.mr.RecordServiceConfig;

/**
 * To run the program:
 * hadoop jar recordservice-examples-*.jar \
 * com.cloudera.recordservice.examples.mapreduce.WordCount <input path> <output path>
 *
 * Append false in the command to run WordCount without RecordService
 */
public class WordCount {
  public static class Map extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value,
         OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        output.collect(word, one);
      }
    }
  }

  public static class Reduce extends MapReduceBase
      implements Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public void run(String[] args) throws Exception {
    boolean useRecordService = true;
    if (args.length == 3) {
      useRecordService =  Boolean.parseBoolean(args[2]);
    } else if (args.length != 2) {
      System.err.println("Usage: WordCount <input path> <output path>");
      System.exit(-1);
    }
    String input = args[0].trim();
    String output = args[1];

    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName(
        "wordcount-" + (useRecordService ? "with" : "without") + "-RecordService");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    if (useRecordService) {
      conf.setInputFormat(com.cloudera.recordservice.mapred.TextInputFormat.class);
      RecordServiceConfig.setInput(conf, input);
    } else {
      conf.setInputFormat(TextInputFormat.class);
      FileInputFormat.setInputPaths(conf, new Path(input));
    }

    FileSystem fs = FileSystem.get(conf);
    Path outputPath = new Path(output);
    if (fs.exists(outputPath)) fs.delete(outputPath, true);
    conf.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(conf, outputPath);

    JobClient.runJob(conf);
    System.out.println("Done");
  }

  public static void main(String[] args) throws Exception {
    new WordCount().run(args);
  }
}
