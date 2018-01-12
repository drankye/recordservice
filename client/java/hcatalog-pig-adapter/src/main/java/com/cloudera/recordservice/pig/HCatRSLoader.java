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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cloudera.recordservice.pig;

import com.cloudera.recordservice.hcatalog.common.HCatRSUtil;
import com.cloudera.recordservice.hcatalog.mapreduce.HCatRSInputFormat;
import com.cloudera.recordservice.hcatalog.mapreduce.HCatTableInfo;
import com.cloudera.recordservice.hcatalog.mapreduce.InputJobInfo;
import com.cloudera.recordservice.hcatalog.mapreduce.PartInfo;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.security.Credentials;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatContext;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.pig.HCatLoader;
import org.apache.pig.Expression;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * Pig {@link org.apache.pig.LoadFunc} to read data from HCat
 *
 * This Class was copied from the Hcatalog-Pig-Adapter Project
 * Original name: HCatLoader
 * Changes:
 *  - Now extends HCatLoader instead of HCatBaseLoader,
 *  - InputFormat has been changed to a RecordServiceInputFormat
 *  - getNext function is overridden to use RecordServiceRecord
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatRSLoader extends HCatLoader {
  private static final Logger LOG = LoggerFactory.getLogger(HCatRSLoader.class);

  private static final String PARTITION_FILTER = "partition.filter"; // for future use
  private HCatRSInputFormat hcatRSInputFormat = null;
  private RecordReader<?, ?> reader;
  private String hcatServerUri;
  private HCatSchema outputSchema = null;
  private String partitionFilterString;
  private final PigHCatUtil phutil = new PigHCatUtil();

  // Signature for wrapped loader, see comments in LoadFuncBasedInputDriver.initialize
  final public static String INNER_SIGNATURE = "hcatloader.inner.signature";
  final public static String INNER_SIGNATURE_PREFIX = "hcatloader_inner_signature";
  // A hash map which stores job credentials. The key is a signature passed by Pig,
  // which is unique to the load func and input file name (table, in our case).
  private static Map<String, Credentials> jobCredentials =
      new HashMap<String, Credentials>();

  @Override
  public InputFormat<?, ?> getInputFormat() throws IOException {
    if (hcatRSInputFormat == null) {
      hcatRSInputFormat = new HCatRSInputFormat();
    }
    return hcatRSInputFormat;
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    HCatContext.INSTANCE.setConf(job.getConfiguration()).getConf().get()
      .setBoolean(HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION, true);
    UDFContext udfContext = UDFContext.getUDFContext();
    Properties udfProps = udfContext.getUDFProperties(this.getClass(),
        new String[]{signature});
    job.getConfiguration().set(INNER_SIGNATURE, INNER_SIGNATURE_PREFIX + "_" + signature);

    RequiredFieldList requiredFieldsInfo = (RequiredFieldList) udfProps
      .get(PRUNE_PROJECTION_INFO);
    // get partitionFilterString stored in the UDFContext - it would have
    // been stored there by an earlier call to setPartitionFilter
    // call setInput on HCatInputFormat only in the frontend because internally
    // it makes calls to the hcat server - we don't want these to happen in
    // the backend
    // in the hadoop front end mapred.task.id property will not be set in
    // the Configuration
    if (udfProps.containsKey(HCatConstants.HCAT_PIG_LOADER_LOCATION_SET)) {
      for (Enumeration<Object> emr = udfProps.keys(); emr.hasMoreElements(); ) {
        PigHCatUtil.getConfigFromUDFProperties(udfProps,
            job.getConfiguration(), emr.nextElement().toString());
      }
      if (!HCatUtil.checkJobContextIfRunningFromBackend(job)) {
        //Combine credentials and credentials from job takes precedence for freshness
        Credentials crd = jobCredentials.get(INNER_SIGNATURE_PREFIX + "_" + signature);
        job.getCredentials().addAll(crd);
      }
    } else {
      Job clone = new Job(job.getConfiguration());
      HCatRSInputFormat.setInput(job, location, getPartitionFilterString());
      InputJobInfo inputJobInfo = (InputJobInfo) HCatRSUtil.deserialize(
          job.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO));

      // TODO: Add back special cases call when I find out where the code has moved.
      addSpecialCasesParametersForHCatLoader(job.getConfiguration(),
          inputJobInfo.getTableInfo());


      // We will store all the new /changed properties in the job in the
      // udf context, so the the HCatInputFormat.setInput method need not
      //be called many times.
      for (Entry<String, String> keyValue : job.getConfiguration()) {
        String oldValue = clone.getConfiguration().getRaw(keyValue.getKey());
        if ((oldValue == null) || (keyValue.getValue().equals(oldValue) == false)) {
          udfProps.put(keyValue.getKey(), keyValue.getValue());
        }
      }
      udfProps.put(HCatConstants.HCAT_PIG_LOADER_LOCATION_SET, true);
      //Store credentials in a private hash map and not the udf context to
      // make sure they are not public.
      Credentials crd = new Credentials();
      crd.addAll(job.getCredentials());
      jobCredentials.put(INNER_SIGNATURE_PREFIX + "_" + signature, crd);
      clone.setInputFormatClass(HCatRSInputFormat.class);
    }

    // Need to also push projections by calling setOutputSchema on
    // HCatInputFormat - we have to get the RequiredFields information
    // from the UdfContext, translate it to an Schema and then pass it
    // The reason we do this here is because setLocation() is called by
    // Pig runtime at InputFormat.getSplits() and
    // InputFormat.createRecordReader() time - we are not sure when
    // HCatInputFormat needs to know about pruned projections - so doing it
    // here will ensure we communicate to HCatInputFormat about pruned
    // projections at getSplits() and createRecordReader() time

    if (requiredFieldsInfo != null) {
      // convert to hcatschema and pass to HCatInputFormat
      try {
        outputSchema = phutil.getHCatSchema(requiredFieldsInfo.getFields(),
            signature, this.getClass());
        HCatRSInputFormat.setOutputSchema(job, outputSchema);
      } catch (Exception e) {
        throw new IOException(e);
      }
    } else {
      // else - this means pig's optimizer never invoked the pushProjection
      // method - so we need all fields and hence we should not call the
      // setOutputSchema on HCatInputFormat
      if (HCatUtil.checkJobContextIfRunningFromBackend(job)) {
        try {
          HCatSchema hcatTableSchema =
              (HCatSchema) udfProps.get(HCatConstants.HCAT_TABLE_SCHEMA);
          outputSchema = hcatTableSchema;
          HCatRSInputFormat.setOutputSchema(job, outputSchema);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    }
    if(LOG.isDebugEnabled()) {
      LOG.debug("outputSchema=" + outputSchema);
    }
    job.setInputFormatClass(HCatRSInputFormat.class);
  }

  @Override
  public String[] getPartitionKeys(String location, Job job)
    throws IOException {
    Table table = phutil.getTable(location,
      hcatServerUri != null ? hcatServerUri : PigHCatUtil.getHCatServerUri(job),
      PigHCatUtil.getHCatServerPrincipal(job),
      job);   // Pass job to initialize metastore conf overrides
    List<FieldSchema> tablePartitionKeys = table.getPartitionKeys();
    String[] partitionKeys = new String[tablePartitionKeys.size()];
    for (int i = 0; i < tablePartitionKeys.size(); i++) {
      partitionKeys[i] = tablePartitionKeys.get(i).getName();
    }
    return partitionKeys;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    HCatContext.INSTANCE.setConf(job.getConfiguration()).getConf().get()
      .setBoolean(HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION, true);
    Table table = phutil.getTable(location,
      hcatServerUri != null ? hcatServerUri : PigHCatUtil.getHCatServerUri(job),
      PigHCatUtil.getHCatServerPrincipal(job),

      // Pass job to initialize metastore conf overrides for embedded metastore case
      // (hive.metastore.uris = "").
      job);
    HCatSchema hcatTableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
    try {
      PigHCatUtil.validateHCatTableSchemaFollowsPigRules(hcatTableSchema);
    } catch (IOException e) {
      throw new PigException(
        "Table schema incompatible for reading through HCatLoader :" + e.getMessage()
          + ";[Table schema was " + hcatTableSchema.toString() + "]"
        , PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
    storeInUDFContext(signature, HCatConstants.HCAT_TABLE_SCHEMA, hcatTableSchema);
    outputSchema = hcatTableSchema;
    return PigHCatUtil.getResourceSchema(hcatTableSchema);
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    // convert the partition filter expression into a string expected by
    // hcat and pass it in setLocation()

    partitionFilterString = getHCatComparisonString(partitionFilter);

    // store this in the udf context so we can get it later
    storeInUDFContext(signature,
      PARTITION_FILTER, partitionFilterString);
  }

  /**
   * Get statistics about the data to be loaded. Only input data size is implemented
   * at this time.
   */
  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    try {
      ResourceStatistics stats = new ResourceStatistics();
      InputJobInfo inputJobInfo = (InputJobInfo) HCatRSUtil.deserialize(
        job.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO));
      stats.setmBytes(getSizeInBytes(inputJobInfo) / 1024 / 1024);
      return stats;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private String getPartitionFilterString() {
    if (partitionFilterString == null) {
      Properties props = UDFContext.getUDFContext().getUDFProperties(
        this.getClass(), new String[]{signature});
      partitionFilterString = props.getProperty(PARTITION_FILTER);
    }
    return partitionFilterString;
  }

  private String getHCatComparisonString(Expression expr) {
    if (expr instanceof BinaryExpression) {
      // call getHCatComparisonString on lhs and rhs, and and join the
      // results with OpType string

      // we can just use OpType.toString() on all Expression types except
      // Equal, NotEqualt since Equal has '==' in toString() and
      // we need '='
      String opStr = null;
      switch (expr.getOpType()) {
      case OP_EQ:
        opStr = " = ";
        break;
      default:
        opStr = expr.getOpType().toString();
      }
      BinaryExpression be = (BinaryExpression) expr;
      return "(" + getHCatComparisonString(be.getLhs()) +
        opStr +
        getHCatComparisonString(be.getRhs()) + ")";
    } else {
      // should be a constant or column
      return expr.toString();
    }
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      RecordServiceRecord record = (RecordServiceRecord)
          (reader.nextKeyValue() ? reader.getCurrentValue() : null);
      Tuple t = PigHCatUtil.transformToTuple(record);
      // TODO : we were discussing an iter interface, and also a LazyTuple
      // change this when plans for that solidifies.
      return t;
    } catch (ExecException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode,
              PigException.REMOTE_ENVIRONMENT, e);
    } catch (Exception eOther) {
      int errCode = 6018;
      String errMsg = "Error converting read value to tuple";
      throw new ExecException(errMsg, errCode,
              PigException.REMOTE_ENVIRONMENT, eOther);
    }
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit arg1) throws IOException {
    this.reader = reader;
  }

  public static void addSpecialCasesParametersForHCatLoader(
          Configuration conf, HCatTableInfo tableInfo) {
    if ((tableInfo == null) || (tableInfo.getStorerInfo() == null)){
      return;
    }
    String shClass = tableInfo.getStorerInfo().getStorageHandlerClass();
    if ((shClass != null) && shClass.equals(
        "org.apache.hadoop.hive.hbase.HBaseStorageHandler")){
      // NOTE: The reason we use a string name of the hive hbase handler here is
      // because we do not want to introduce a compile-dependency on the
      // hive-hbase-handler module from within hive-hcatalog.
      // This parameter was added due to the requirement in HIVE-7072
      conf.set("pig.noSplitCombination", "true");
    }
  }

  /**
   * A utility method to get the size of inputs. This is accomplished by summing the
   * size of all input paths on supported FileSystems. Locations whose size cannot be
   * determined are ignored. Note non-FileSystem and unpartitioned locations will not
   * report their input size by default. This method was copied from HcatBaseLoader to use
   * the Record Service InputJobInfo.
   */
  protected static long getSizeInBytes(InputJobInfo inputJobInfo) throws IOException {
    Configuration conf = new Configuration();
    long sizeInBytes = 0;

    for (PartInfo partInfo : inputJobInfo.getPartitions()) {
      try {
        Path p = new Path(partInfo.getLocation());
        if (p.getFileSystem(conf).isFile(p)) {
          sizeInBytes += p.getFileSystem(conf).getFileStatus(p).getLen();
        } else {
          FileStatus[] fileStatuses = p.getFileSystem(conf).listStatus(p);
          if (fileStatuses != null) {
            for (FileStatus child : fileStatuses) {
              sizeInBytes += child.getLen();
            }
          }
        }
      } catch (IOException e) {
        // Report size to the extent possible.
      }
    }
    LOG.info("SIZE:" + sizeInBytes + "\n\n");
    return sizeInBytes;
  }

}
