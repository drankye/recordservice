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

package com.cloudera.recordservice.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.Credentials;
import org.junit.Test;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.TestBase;
import com.cloudera.recordservice.mapreduce.testapps.RecordCount;
import com.cloudera.recordservice.mr.DecimalWritable;
import com.cloudera.recordservice.mr.PlanUtil;
import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.cloudera.recordservice.mr.RecordServiceConfig.ConfVars;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import com.cloudera.recordservice.mr.TimestampNanosWritable;

public class MapReduceTest extends TestBase {

  private void verifyInputSplits(int numSplits, int numCols, Configuration config)
      throws IOException {
    List<InputSplit> splits = PlanUtil.getSplits(config, new Credentials()).splits;
    assertEquals(numSplits, splits.size());
    RecordServiceInputSplit split = (RecordServiceInputSplit)splits.get(0);
    assertEquals(numCols, split.getSchema().getNumColumns());
  }

  private void verifyException(String msg,
      String db, String tbl, String... columns) {
    Configuration config = new Configuration();
    boolean exceptionThrown = false;
    try {
      RecordServiceConfig.setInputTable(config, db, tbl, columns);
      PlanUtil.getSplits(config, new Credentials());
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(msg));
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(msg));
    }
    assertTrue(exceptionThrown);
  }

  private void verifyInputSplitsTable(int numSplits, int numCols,
      String tbl, String... cols) throws IOException {
    Configuration config = new Configuration();
    config.set(ConfVars.TBL_NAME_CONF.name, tbl);
    if (cols.length > 0) {
      config.setStrings(ConfVars.COL_NAMES_CONF.name, cols);
    }
    verifyInputSplits(numSplits, numCols, config);
  }

  private void verifyInputSplitsPath(int numSplits, int numCols, String path)
      throws IOException {
    Configuration config = new Configuration();
    config.set(FileInputFormat.INPUT_DIR, path);
    verifyInputSplits(numSplits, numCols, config);
  }

  @Test
  public void testGetSplits() throws IOException {
    Configuration config = new Configuration();

    boolean exceptionThrown = false;
    try {
      PlanUtil.getSplits(config, new Credentials());
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("No input specified"));
    }
    assertTrue(exceptionThrown);

    // Set db/table and make sure it works.
    config.set(ConfVars.TBL_NAME_CONF.name, "tpch.nation");
    PlanUtil.getSplits(config, new Credentials());

    // Also set input. This should fail.
    config.set(FileInputFormat.INPUT_DIR, "/test");
    exceptionThrown = false;
    try {
      PlanUtil.getSplits(config, new Credentials());
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains(
          "More than one input specified"));
    }
    assertTrue(exceptionThrown);

    // Unset the table and set columns. INPUT_DIR and columns don't work now.
    config.unset(ConfVars.TBL_NAME_CONF.name);
    config.setStrings(ConfVars.COL_NAMES_CONF.name, "a");
    exceptionThrown = false;
    try {
      PlanUtil.getSplits(config, new Credentials());
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains(
          "Column projections can only be specified with table inputs."));
    }
    assertTrue(exceptionThrown);

    // Test some cases that work
    verifyInputSplitsTable(1, 4, "tpch.nation");
    verifyInputSplitsTable(2, 12, "rs.alltypes");
    verifyInputSplitsTable(1, 1, "tpch.nation", "n_name");
    verifyInputSplitsTable(2, 3, "rs.alltypes", "int_col", "double_col", "string_col");
    verifyInputSplitsPath(1, 1, "/test-warehouse/tpch.nation");

    // Test some cases using the config utility.
    config.clear();
    RecordServiceConfig.setInputTable(config, null,
        "tpch.nation", "n_nationkey", "n_comment");
    verifyInputSplits(1, 2, config);

    exceptionThrown = false;
    try {
      verifyInputSplitsTable(1, 1, "tpch.nation", "bad");
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause() instanceof RecordServiceException);
      RecordServiceException ex = (RecordServiceException)e.getCause();
      assertEquals(RecordServiceException.ErrorCode.INVALID_REQUEST, ex.code);
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      verifyInputSplitsPath(1, 1,
          "/test-warehouse/tpch.nation,/test-warehouse/tpch.nation");
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains(
          "Only reading a single directory is currently supported."));
    }
    assertTrue(exceptionThrown);
  }

  @Test
  // TODO: make this generic. This should be extensible to test all the input
  // formats we support. How do we do this?
  public void testReadNation() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    RecordServiceInputFormat.RecordServiceRecordReader reader =
        new RecordServiceInputFormat.RecordServiceRecordReader();

    try {
      RecordServiceConfig.setInputTable(config, null, "tpch.nation");
      List<InputSplit> splits = PlanUtil.getSplits(config, new Credentials()).splits;
      reader.initialize(splits.get(0),
          new TaskAttemptContextImpl(new JobConf(config), new TaskAttemptID()));

      int numRows = 0;
      while (reader.nextKeyValue()) {
        RecordServiceRecord value = reader.getCurrentValue();
        ++numRows;

        if (numRows == 10) {
          assertEquals("INDONESIA", value.getColumnValue(1).toString());
        }
      }
      assertFalse(reader.nextKeyValue());
      assertFalse(reader.nextRecord());
      assertEquals(25, numRows);

      config.clear();
      RecordServiceConfig.setInputTable(config, "tpch", "nation", "n_comment");
      splits = PlanUtil.getSplits(config, new Credentials()).splits;
      reader.initialize(splits.get(0),
          new TaskAttemptContextImpl(new JobConf(config), new TaskAttemptID()));
      numRows = 0;
      while (reader.nextKeyValue()) {
        RecordServiceRecord value = reader.getCurrentValue();
        if (numRows == 12) {
          assertEquals("ously. final, express gifts cajole a",
              value.getColumnValue(0).toString());
        }
        ++numRows;
      }
      assertEquals(25, numRows);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReadAllTypes() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    RecordServiceInputFormat.RecordServiceRecordReader reader =
        new RecordServiceInputFormat.RecordServiceRecordReader();

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));

    try {
      RecordServiceConfig.setInputTable(config, null, "rs.alltypes");
      List<InputSplit> splits = PlanUtil.getSplits(config, new Credentials()).splits;

      int numRows = 0;
      for (InputSplit split: splits) {
        reader.initialize(split,
            new TaskAttemptContextImpl(new JobConf(config), new TaskAttemptID()));
        while (reader.nextKeyValue()) {
          RecordServiceRecord value = reader.getCurrentValue();
          if (((BooleanWritable)value.getColumnValue(0)).get()) {
            assertEquals(0, ((ByteWritable)value.getColumnValue(1)).get());
            assertEquals(1, ((ShortWritable)value.getColumnValue(2)).get());
            assertEquals(2, ((IntWritable)value.getColumnValue(3)).get());
            assertEquals(3, ((LongWritable)value.getColumnValue(4)).get());
            assertEquals(4.0, ((FloatWritable)value.getColumnValue(5)).get(), 0.1);
            assertEquals(5.0, ((DoubleWritable)value.getColumnValue(6)).get(), 0.1);
            assertEquals("hello", value.getColumnValue(7).toString());
            assertEquals("vchar1", value.getColumnValue(8).toString());
            assertEquals("char1", value.getColumnValue(9).toString());
            assertEquals("2015-01-01", format.format(
                ((TimestampNanosWritable)value.getColumnValue(10)).get().toTimeStamp()));
            assertEquals(
                new BigDecimal("3.1415920000"),
                ((DecimalWritable)value.getColumnValue(11)).get().toBigDecimal());
          } else {
            assertEquals(6, ((ByteWritable)value.getColumnValue(1)).get());
            assertEquals(7, ((ShortWritable)value.getColumnValue(2)).get());
            assertEquals(8, ((IntWritable)value.getColumnValue(3)).get());
            assertEquals(9, ((LongWritable)value.getColumnValue(4)).get());
            assertEquals(10.0, ((FloatWritable)value.getColumnValue(5)).get(), 0.1);
            assertEquals(11.0, ((DoubleWritable)value.getColumnValue(6)).get(), 0.1);
            assertEquals("world", value.getColumnValue(7).toString());
            assertEquals("vchar2", value.getColumnValue(8).toString());
            assertEquals("char2", value.getColumnValue(9).toString());
            assertEquals("2016-01-01",
                format.format(
                    ((TimestampNanosWritable)value.getColumnValue(10))
                        .get().toTimeStamp()));
            assertEquals(
                new BigDecimal("1234.5678900000"),
                ((DecimalWritable)value.getColumnValue(11)).get().toBigDecimal());
          }
          ++numRows;
        }
      }
      assertEquals(2, numRows);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReadAllTypesNull() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    RecordServiceInputFormat.RecordServiceRecordReader reader =
        new RecordServiceInputFormat.RecordServiceRecordReader();

    try {
      RecordServiceConfig.setInputTable(config, null, "rs.alltypes_null");
      List<InputSplit> splits = PlanUtil.getSplits(config, new Credentials()).splits;

      int numRows = 0;
      for (InputSplit split: splits) {
        reader.initialize(split,
            new TaskAttemptContextImpl(new JobConf(config), new TaskAttemptID()));
        while (reader.nextKeyValue()) {
          RecordServiceRecord value = reader.getCurrentValue();
          for (int i = 0; i < value.getSchema().getNumColumns(); ++i) {
            assertTrue(value.getColumnValue(i) == null);
          }
          ++numRows;
        }
      }
      assertEquals(1, numRows);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testCountStar() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    TextInputFormat.TextRecordReader reader =
        new TextInputFormat.TextRecordReader();

    try {
      RecordServiceConfig.setInputQuery(config, "select count(*) from tpch.nation");
      List<InputSplit> splits = PlanUtil.getSplits(config, new Credentials()).splits;
      int numRows = 0;
      for (InputSplit split: splits) {
        reader.initialize(split,
            new TaskAttemptContextImpl(new JobConf(config), new TaskAttemptID()));
        while (reader.nextKeyValue()) {
          ++numRows;
        }
      }
      assertEquals(25, numRows);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testConfigs() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    RecordServiceConfig.setInputTable(config, "tpch", "nation", (String[])null);
    verifyInputSplits(1, 4, config);
    verifyException("Could not resolve table reference", "", "nation", "n_comment");
    verifyException("'tbl' must be non-empty", "tpch", null, "n_comment");
    verifyException("'tbl' must be non-empty", "tpch", "", "n_comment");
    verifyException("Column list cannot contain empty names.",
        "tpch", "nation", "n_comment", null);
    verifyException("Column list cannot contain empty names.",
        "tpch", "nation", "n_comment", "");
    verifyException("Column list cannot contain empty names.",
        "tpch", "nation", "");

    List<NetworkAddress> hostports =
        RecordServiceConfig.getPlannerHostPort("a:1234,b:12");
    assertEquals(2, hostports.size());
    assertEquals("a", hostports.get(0).hostname);
    assertEquals(1234, hostports.get(0).port);
    assertEquals("b", hostports.get(1).hostname);
    assertEquals(12, hostports.get(1).port);
  }

  @Test
  public void testJobs() throws IOException {
    assertEquals(25, RecordCount.countRecords("hdfs:/test-warehouse/tpch.nation"));
  }
}
