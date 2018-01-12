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
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.recordservice.mr.RecordServiceConfig;

/**
 * Avro / Mapred Color Count Example with Generic Record.
 */
public class MapredColorCount extends Configured implements Tool {

  public static class ColorCountMapper extends AvroMapper<GenericData.Record,
      Pair<CharSequence, Integer>> {
    private final static Pair<CharSequence, Integer> PAIR =
        new Pair<CharSequence, Integer>("", 1);

    @Override
    public void map(GenericData.Record user,
        AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
        throws IOException {
      CharSequence color = (CharSequence) user.get("favorite_color");
      // We need this check because the User.favorite_color field has
      // type ["string", "null"]
      if (color == null) {
        color = "none";
      }

      PAIR.key(color);
      collector.collect(PAIR);
    }
  }

  public static class ColorCountReducer extends AvroReducer<CharSequence, Integer,
      Pair<CharSequence, Integer>> {
    private final static Pair<CharSequence, Integer> PAIR =
        new Pair<CharSequence, Integer>("", 1);

    @Override
    public void reduce(CharSequence key, Iterable<Integer> values,
        AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
        throws IOException {
      int sum = 0;
      for (Integer value : values) {
        sum += value;
      }

      PAIR.set(key, sum);
      collector.collect(PAIR);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    org.apache.log4j.BasicConfigurator.configure();

    if (args.length != 2) {
      System.err.println("Usage: MapredColorCount <input path> <output path>");
      return -1;
    }

    JobConf conf = new JobConf(getConf(), MapredColorCount.class);
    conf.setJobName("colorcount With Generic Records");

    // RECORDSERVICE:
    // By using the recordservice AvroJob utility, we can configure at run time to
    // switch between using the recordservice or not.
    // In this example, we'll set the conf to true to enable the RecordService..
    conf.setBoolean(
        com.cloudera.recordservice.avro.AvroJob.USE_RECORD_SERVICE_INPUT_FORMAT_CONF_KEY,
        true);
    com.cloudera.recordservice.avro.AvroJob.setInputFormat(conf,
        org.apache.avro.mapred.AvroInputFormat.class);

    // RECORDSERVICE:
    // To read from a table instead of a path, comment out setInputPaths and instead use:
    RecordServiceConfig.setInputTable(conf, "rs", "users");
    //FileInputFormat.setInputPaths(conf, new Path(args[0]));

    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    AvroJob.setMapperClass(conf, ColorCountMapper.class);
    AvroJob.setReducerClass(conf, ColorCountReducer.class);

    // Note that AvroJob.setOutputSchema set relevant config options such as output
    // format, map output classes, and output key class.
    // Do not need to setInputSchema when using Generic Records.
    AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT)));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapredColorCount(), args);
    System.exit(res);
  }
}