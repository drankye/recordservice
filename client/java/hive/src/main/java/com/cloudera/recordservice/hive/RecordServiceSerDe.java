/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.recordservice.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

import com.cloudera.recordservice.mr.RecordServiceRecord;

/**
 * A SerDe that allows Hive to interpret data returned by the RecordService.
 * Expected to be used with RecordServiceInputFormat.
 *
 * A SerDe is composed of two main parts:
 * 1) The serialization/deserialization logic (handles data passed back from a
 *    RecordReader). In the RecordServiceSerDe case, this is a no-op because the
 *    row is already structured and has been parsed.
 * 2) An "ObjectInspector" which is a standard interface Hive uses to extract
 *    data from a row (struct) and a field (column). Also used to get column
 *    metadata that might be specific to the SerDe.
 *
 * To create a table with this SerDe use:
 * CREATE EXTERNAL TABLE jointbl_rs_serde (<col list>)
 * ROW FORMAT SERDE 'com.cloudera.recordservice.hive.RecordServiceSerDe'
 * STORED AS INPUTFORMAT 'com.cloudera.recordservice.mapred.RecordServiceInputFormat'
 * OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
 * LOCATION '<hdfs path>'
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class RecordServiceSerDe extends AbstractSerDe {
  public static final Log LOG = LogFactory.getLog(RecordServiceSerDe.class);

  // ObjectInspector for extracting column values from records.
  private RecordServiceObjectInspector objInspector_;

  // The names of all columns in the table (not the projection), in order of their
  // position in the table.
  private List<String> columnNames_;

  /**
   * Initializes the RecordServiceSerde based on the table schema.
   */
  @Override
  public final void initialize(final Configuration conf, final Properties tbl)
      throws SerDeException {
    final List<TypeInfo> columnTypes;
    final String columnNameProperty = tbl.getProperty(IOConstants.COLUMNS);
    final String columnTypeProperty = tbl.getProperty(IOConstants.COLUMNS_TYPES);

    if (columnNameProperty.length() == 0) {
      columnNames_ = new ArrayList<String>();
    } else {
      columnNames_ = Arrays.asList(columnNameProperty.split(","));
    }

    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (columnNames_.size() != columnTypes.size()) {
      throw new IllegalArgumentException("Initialization failed. Number of column " +
        "names and column types differs. columnNames = " + columnNames_ +
        ", columnTypes = " + columnTypes);
    }

    // Initialize the ObjectInspector based on the column type information in the table.
    final TypeInfo rowTypeInfo =
        TypeInfoFactory.getStructTypeInfo(columnNames_, columnTypes);
    objInspector_ = new RecordServiceObjectInspector((StructTypeInfo) rowTypeInfo);
  }

  /**
   * Deserialize a record returned by a RecordReader. The incoming record already has
   * been parsed into columns and has a schema, so it can be handed off to the
   * ObjectInspector with no work.
   * On the hot path - called for every record returned by the RecordReader.
   */
  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    return blob;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objInspector_;
  }

  /**
   * The class that is being serialized in "serialize()". Not currently used.
   */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return RecordServiceRecord.class;
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector objInspector)
      throws SerDeException {
    // TODO: Support writing data out.
    throw new UnsupportedOperationException("Not Yet Implemented");
  }

  @Override
  public SerDeStats getSerDeStats() { return null; }
}