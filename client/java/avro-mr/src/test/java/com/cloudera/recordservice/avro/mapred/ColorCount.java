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

package com.cloudera.recordservice.avro.mapred;

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
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.recordservice.core.TestUtil;
import com.cloudera.recordservice.mr.RecordServiceConfig;

/**
 * Mapred application that just counts the color in a data set.
 * Can be run with or without record service.
 */
public class ColorCount {
  public static class Map extends AvroMapper<GenericData.Record,
      Pair<CharSequence, Integer>> {
    private final static Pair<CharSequence, Integer> PAIR =
        new Pair<CharSequence, Integer>("", 1);

    @Override
    public void map(GenericData.Record user,
        AvroCollector<Pair<CharSequence, Integer>> collector, Reporter reporter)
        throws IOException {
      CharSequence color = (CharSequence) user.get("favorite_color");
      if (color == null) {
        color = "none";
      }

      PAIR.key(color);
      collector.collect(PAIR);
    }
  }

  public static class Reduce extends AvroReducer<CharSequence, Integer,
      Pair<CharSequence, Integer>> {
    private final static Pair<CharSequence, Integer> PAIR =
        new Pair<CharSequence, Integer>("", 0);

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

  /**
   * Run the MR1 color count with generic records, and return a map of favorite colors to
   * the number of users.
   */
  public static java.util.Map<String, Integer> countColors() throws IOException {
    String output = TestUtil.getTempDirectory();
    Path outputPath = new Path(output);

    JobConf conf = new JobConf(ColorCount.class);
    conf.setJobName("MR1 Color Count With Generic Records");
    conf.setInt("mapreduce.job.reduces", 1);

    conf.setBoolean(
        com.cloudera.recordservice.avro.AvroJob.USE_RECORD_SERVICE_INPUT_FORMAT_CONF_KEY,
        true);
    com.cloudera.recordservice.avro.AvroJob.setInputFormat(conf,
        org.apache.avro.mapred.AvroInputFormat.class);

    RecordServiceConfig.setInputTable(conf, "rs", "users");
    FileOutputFormat.setOutputPath(conf, outputPath);

    AvroJob.setMapperClass(conf, Map.class);
    AvroJob.setReducerClass(conf, Reduce.class);
    AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT)));

    JobClient.runJob(conf);

    // Read the result and return it. Since we set the number of reducers to 1,
    // there is always just one file containing the value.
    SeekableInput input = new FsInput(new Path(output + "/part-00000.avro"), conf);
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    FileReader<GenericRecord> fileReader = DataFileReader.openReader(input, reader);
    java.util.Map<String, Integer> colorMap = new HashMap<String, Integer>();
    for (GenericRecord datum: fileReader) {
      colorMap.put(datum.get(0).toString(), Integer.parseInt(datum.get(1).toString()));
    }
    return colorMap;
  }
}
