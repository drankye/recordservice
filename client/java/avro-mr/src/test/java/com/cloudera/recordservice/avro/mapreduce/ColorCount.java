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

package com.cloudera.recordservice.avro.mapreduce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cloudera.recordservice.core.TestUtil;
import com.cloudera.recordservice.mr.RecordServiceConfig;

/**
 * MapReduce application that just counts the color in a data set.
 * Can be run with or without record service.
 */
public class ColorCount {
  public static class Map extends
      Mapper<AvroKey<GenericData.Record>, NullWritable, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text COLOR = new Text();

    @Override
    public void map(AvroKey<GenericData.Record> key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      CharSequence color = (CharSequence)key.datum().get("favorite_color");
      if (color == null) {
        color = "none";
      }
      COLOR.set(color.toString());
      context.write(COLOR, ONE);
    }
  }

  public static class Reduce extends
      Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
    private final static AvroKey<CharSequence> KEY = new AvroKey<CharSequence>();
    private final static AvroValue<Integer> VALUE = new AvroValue<Integer>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      KEY.datum(key.toString());
      VALUE.datum(sum);
      context.write(KEY, VALUE);
    }
  }

  /**
   * Run the MR2 color count with generic records, and return a map of favorite colors to
   * the number of users.
   */
  public static java.util.Map<String, Integer> countColors() throws IOException,
      ClassNotFoundException, InterruptedException {
    String output = TestUtil.getTempDirectory();
    Path outputPath = new Path(output);
    JobConf conf = new JobConf(ColorCount.class);
    conf.setInt("mapreduce.job.reduces", 1);

    Job job = Job.getInstance(conf);
    job.setJarByClass(ColorCount.class);
    job.setJobName("MR2 Color Count With Generic Records");

    RecordServiceConfig.setInputTable(job.getConfiguration(), "rs", "users");
    job.setInputFormatClass(
        com.cloudera.recordservice.avro.mapreduce.AvroKeyInputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setMapperClass(Map.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    job.setReducerClass(Reduce.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

    job.waitForCompletion(false);

    // Read the result and return it. Since we set the number of reducers to 1,
    // there is always just one file containing the value.
    SeekableInput input = new FsInput(new Path(output + "/part-r-00000.avro"), conf);
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
    java.util.Map<String, Integer> colorMap = new HashMap<String, Integer>();
    for (GenericRecord datum: fileReader) {
      colorMap.put(datum.get(0).toString(), Integer.parseInt(datum.get(1).toString()));
    }
    return colorMap;
  }
}
