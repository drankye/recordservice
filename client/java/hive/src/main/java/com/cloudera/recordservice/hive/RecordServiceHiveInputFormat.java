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

package com.cloudera.recordservice.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveRecordReader;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import com.cloudera.recordservice.mapred.RecordServiceInputFormat;
import com.cloudera.recordservice.mapred.RecordServiceInputFormat.RecordServiceRecordReader;
import com.cloudera.recordservice.mapred.RecordServiceInputSplit;
import com.google.common.base.Joiner;

/**
 * A HiveInputFormat that redirects all HDFS reads to the RecordService. This is
 * done by extracting the Hive query plan and finding the TableScanOperators. From there
 * we know the table being scanned and the columns being projected. A
 * RecordServiceRecordReader is then passed to the HiveRecordReader instead of the
 * table's actual InputFormat (in getRecordReader())
 *
 * This can be enabled to happen transparently by setting this class as the
 * default hive input format. To do this put the following in the hive-site.xml:
 *
 * <property>
 *   <name>hive.input.format</name>
 *   <value>com.cloudera.recordservice.hive.RecordServiceHiveInputFormat</value>
 * </property>
 *
 * There are still some issues with this implementation:
 * - It doesn't handle multiple table scans of different tables.
 * - Hive short-circuits some statements like SELECT * (it just cats the file). This
 *   statement will not go through the RecordService.
 * - Results may be returned to the Hive client as NULL in some cases since our
 *   RecordReader is a hack.
 * - Queries that don't specify any columns and multi-table joins probably don't work.
 */
@SuppressWarnings("rawtypes")
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class RecordServiceHiveInputFormat<K extends WritableComparable,
    V extends Writable> extends HiveInputFormat<K, V> {

  private static final Log LOG =
      LogFactory.getLog(RecordServiceHiveInputFormat.class.getName());

  /**
   * Wrapper around the HiveInputSplit to force it to have the right behavior for
   * InputSplits that are not instances of FileSplit (ie - a RecordServiceInputSplit).
   * TODO: Fix this in Hive.
   */
  public static class HiveInputSplitShim extends HiveInputSplit {
    private Path dummyPath_;

    public HiveInputSplitShim() {
      super();
    }

    public HiveInputSplitShim(Path dummyPath, InputSplit inputSplit,
        String inputFormatClassName) {
      super(inputSplit, inputFormatClassName);
      dummyPath_ = dummyPath;
    }

    @Override
    public Path getPath() {
      if (!(getInputSplit() instanceof RecordServiceInputSplit)) {
        return super.getPath();
      }
      return dummyPath_;
    }

    /**
     * Overridden to bypass some of the Hive I/O layer which was always
     * performing file header checks when getStart() == 0 (which is the default
     * for non-File based splits). This caused a significant perf regression.
     */
    @Override
    public long getStart() {
      if (!(getInputSplit() instanceof RecordServiceInputSplit)) {
        return super.getStart();
      }
      return -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      super.write(out);
      Text.writeString(out, dummyPath_.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      super.readFields(in);
      dummyPath_ = new Path(Text.readString(in));
    }
  }

  /**
   * A HiveRecordReader implementation that is required to bypass some of Hive's I/O
   * management.
   */
  public static class HiveRecordReaderShim<K extends WritableComparable,
      V extends Writable> extends HiveRecordReader<K, V> {
    public HiveRecordReaderShim(RecordReader recordReader) throws IOException {
      super(recordReader);
    }

    public HiveRecordReaderShim(RecordReader recordReader, JobConf conf)
        throws IOException {
      super(recordReader, conf);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean doNext(K key, V value) throws IOException {
      if (ExecMapper.getDone()) {
        return false;
      }
      // Directly invoke the wrapped RecordReader. This is needed for performance reasons
      // (the Hive code was trying to manage some of the I/O).
      // TODO: Investigate this more.
      return super.recordReader.next(key,  value);
    }
  }

  // Includes all information need to run a Hive map task. Exposes utility functions
  // to help with mapping the Hive query plan to the actual work that is being done
  // in the MR job.
  private MapWork mrwork_;

  @Override
  protected void init(JobConf job) {
    super.init(job);
    mrwork_ = Utilities.getMapWork(job);
    pathToPartitionInfo = mrwork_.getPathToPartitionInfo();
  }

  /**
   * Copied HiveInputFormat
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    init(job);

    Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    JobConf newjob = new JobConf(job);
    List<InputSplit> result = new ArrayList<InputSplit>();

    List<Path> currentDirs = new ArrayList<Path>();
    Class<? extends InputFormat> currentInputFormatClass = null;
    TableDesc currentTable = null;
    TableScanOperator currentTableScan = null;

    // for each dir, get the InputFormat, and do getSplits.
    for (Path dir : dirs) {
      PartitionDesc part = getPartitionDescFromPath(pathToPartitionInfo, dir);
      Class<? extends InputFormat> inputFormatClass = part.getInputFileFormatClass();
      TableDesc table = part.getTableDesc();
      TableScanOperator tableScan = null;

      List<String> aliases =
          mrwork_.getPathToAliases().get(dir.toUri().toString());

      // Make filter pushdown information available to getSplits.
      if ((aliases != null) && (aliases.size() == 1)) {
        Operator op = mrwork_.getAliasToWork().get(aliases.get(0));
        if ((op != null) && (op instanceof TableScanOperator)) {
          tableScan = (TableScanOperator) op;
          // push down projections.
          ColumnProjectionUtils.appendReadColumns(
              newjob, tableScan.getNeededColumnIDs(), tableScan.getNeededColumns());
          // push down filters
          pushFilters(newjob, tableScan);
        }
      }

      if (!currentDirs.isEmpty() &&
          inputFormatClass.equals(currentInputFormatClass) &&
          table.equals(currentTable) &&
          tableScan == currentTableScan) {
        currentDirs.add(dir);
        continue;
      }

      if (!currentDirs.isEmpty()) {
        LOG.info("Generating splits");
        addSplitsForGroup(currentDirs, currentTableScan, newjob,
            getInputFormatFromCache(currentInputFormatClass, job),
            currentInputFormatClass, currentDirs.size()*(numSplits / dirs.length),
            currentTable, result);
      }

      currentDirs.clear();
      currentDirs.add(dir);
      currentTableScan = tableScan;
      currentTable = table;
      currentInputFormatClass = inputFormatClass;
    }

    if (dirs.length != 0) {
      LOG.info("Generating splits");
      addSplitsForGroup(currentDirs, currentTableScan, newjob,
          getInputFormatFromCache(currentInputFormatClass, job),
          currentInputFormatClass, currentDirs.size()*(numSplits / dirs.length),
          currentTable, result);
    }

    LOG.info("number of splits " + result.size());
    return result.toArray(new HiveInputSplitShim[result.size()]);
  }

  /*
   * AddSplitsForGroup collects separate calls to setInputPaths into one where possible.
   * Then calls
   */
  private void addSplitsForGroup(List<Path> dirs, TableScanOperator tableScan,
      JobConf conf, InputFormat inputFormat, Class<? extends InputFormat>
      inputFormatClass, int splits, TableDesc table, List<InputSplit> result)
      throws IOException {
    Utilities.copyTableJobPropertiesToConf(table, conf);

    // The table and database name to scan.
    // TODO: This is commented out until we have a pluggable way to configure the
    // SerDe. Until then, the create a separate table and set the job conf properties.
    // String fqTblName[] = table.getTableName().split("\\.");
    //
    // conf.set("recordservice.table.name", table.getTableName());

    if (tableScan != null) {
      pushFilters(conf, tableScan);
      // Set the projected column and table info for the RecordServiceRecordReader.
      conf.set("recordservice.col.names",
          Joiner.on(",").join(tableScan.getNeededColumns()));
    }
    // Unset the file config. We're going to be just reading from the table.
    conf.unset(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR);
    conf.setInputFormat(inputFormat.getClass());

    // Generate the RecordService
    InputSplit[] iss = inputFormat.getSplits(conf, splits);
    for (InputSplit is: iss) {
      if (is instanceof FileSplit) {
        FileSplit fileSplit = (FileSplit) is;
        LOG.info("INPUT SPLIT: " + fileSplit.getPath().toString());
      }
    }

    // Wrap the InputSplits in HiveInputSplits. We use modified version of the
    // HiveInputSplit to work around some issues with the base one.
    // TODO: Get changes incorporated into Hive.
    for (InputSplit is: iss) {
      result.add(new HiveInputSplitShim(dirs.get(0), is, inputFormatClass.getName()));
    }
  }

  /**
   * Replaces whatever the actual InputFormat/RecordReader is with the RecordService
   * version.
   */
  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    // Initialize everything needed to read the projection information.
    HiveInputSplit hsplit = (HiveInputSplit) split;

    if (pathToPartitionInfo == null) init(job);

    PartitionDesc part = pathToPartitionInfo.get(hsplit.getPath().toString());
    if ((part != null) && (part.getTableDesc() != null)) {
      Utilities.copyTableJobPropertiesToConf(part.getTableDesc(), job);
    }

    // Create a RecordServiceRecordReader and wrap it with a HiveRecordReader
    // to read the data.
    RecordServiceRecordReader rsRr = new RecordServiceRecordReader(
        (RecordServiceInputSplit)hsplit.getInputSplit(), job, reporter);

    // Pass the RecordService as the target InputFormat (this will override
    // the actual input format).
    String inputFormatClassName = RecordServiceInputFormat.class.getName();
    Class cl;
    try {
      cl = job.getClassByName(inputFormatClassName);
    } catch (ClassNotFoundException e) {
      throw new IOException("cannot find class " + inputFormatClassName, e);
    }
    HiveRecordReaderShim<K,V> rr = new HiveRecordReaderShim<K, V>(rsRr, job);
    // Initialize the Hive RecordReader IO Context so that it will do the proper thing.
    rr.initIOContext(hsplit, job, cl);
    return rr;
  }
}
