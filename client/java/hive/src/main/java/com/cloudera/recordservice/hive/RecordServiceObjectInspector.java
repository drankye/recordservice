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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

import com.cloudera.recordservice.mr.RecordServiceRecord;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Object Inspector for rows (structs) in a format returned by the RecordService.
 * A key responsibility of the object inspector is to extract column values from
 * a row, mapping them back to Hive column names.
 * ObjectInspectors are hierarchical - There is an object inspector for a row (or struct)
 * and each column (field) has its own object inspector. For complex types, struct
 * object inspectors are used for inspecting fields with struct data types as well as
 * rows.
 * TODO: Complex types are NYI. Support Decimal.
 */
public class RecordServiceObjectInspector extends StructObjectInspector {
  // List of all fields in the table.
  private final List<RecordServiceStructField> fields_;

  // Map of column name to field metadata.
  private final Map<String, RecordServiceStructField> fieldsByName_;

  public RecordServiceObjectInspector(StructTypeInfo rowTypeInfo) {
    List<String> fieldNames = rowTypeInfo.getAllStructFieldNames();
    fields_ = Lists.newArrayListWithExpectedSize(fieldNames.size());
    fieldsByName_ = Maps.newHashMap();

    for (int fieldIdx = 0; fieldIdx < fieldNames.size(); ++fieldIdx) {
      final String name = fieldNames.get(fieldIdx);
      final TypeInfo fieldInfo = rowTypeInfo.getAllStructFieldTypeInfos().get(fieldIdx);
      RecordServiceStructField fieldImpl = new RecordServiceStructField(name,
          getFieldObjectInspector(fieldInfo), fieldIdx);
      fields_.add(fieldImpl);
      fieldsByName_.put(name.toLowerCase(), fieldImpl);
    }
  }

  /**
   * Given a Hive column type, returns the ObjectInspector that will be used to
   * get data from the field. Currently using the the standard Writable object
   * inspectors.
   * TODO: Support all types
   */
  private ObjectInspector getFieldObjectInspector(final TypeInfo typeInfo) {
    if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }  else if (typeInfo instanceof DecimalTypeInfo) {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          (DecimalTypeInfo) typeInfo);
    } else if (typeInfo instanceof VarcharTypeInfo) {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          (VarcharTypeInfo) typeInfo);
    } else if (typeInfo instanceof CharTypeInfo) {
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          (CharTypeInfo) typeInfo);
    } else {
      throw new UnsupportedOperationException("Unknown field type: " + typeInfo);
    }
  }

  /**
   * Given a field name, returns the StructField. Not performance critical.
   */
  @Override
  public StructField getStructFieldRef(String fieldName) {
    return fieldsByName_.get(fieldName.toLowerCase());
  }

  /**
   * Given a field reference, return the data for the specified row.
   * This is on the hot path. It is called once for every column in the schema
   * for every row.
   * TODO: Make this more performant.
   */
  @Override
  public Object getStructFieldData(Object recordData, StructField fieldRef) {
    if (recordData == null) return null;
    RecordServiceStructField field = (RecordServiceStructField) fieldRef;

    // If this field is not selected in the projection, return null.
    if (!field.isProjected()) return null;

    RecordServiceRecord record = (RecordServiceRecord) recordData;

    // Only initialize the projection index if it has not yet been set because
    // this is expensive.
    if (!field.isProjectedColIdxSet()) {
      int colIdx = record.getSchema().getColIdxFromColName(fieldRef.getFieldName());
      field.setProjectedColIdx(colIdx);
      if (!field.isProjected()) return null;
    }
    return record.getColumnValue(field.getProjectedIdx());
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    throw new UnsupportedOperationException("Not Yet Implemented.");
  }

  /**
   * Returns all the fields for this record.
   */
  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields_;
  }

  @Override
  public String getTypeName() { return TypeInfo.class.getName(); }
  @Override
  public Category getCategory() { return Category.STRUCT; }

  /**
   * A description of a field in of a row in RecordService format (similar to the
   * RecordService's TColumnDesc. Also contains the object inspector that will be
   * used to extract the value from this column in a record.
   */
  static class RecordServiceStructField implements StructField {
    private final String fieldName_;
    private final ObjectInspector inspector_;

    // The index of this field in the struct (may not be the same as the index
    // of the field in the RecordService schema).
    private final int fieldIdx_;

    // The index of this field in a projection. -1 if not set or if not records
    // have been received from the RecordService. Will not be set for metadata only
    // operations (ie DESCRIBE TABLE).
    private int projectedColIdx_ = -1;

    boolean isProjectedColIdxSet_ = false;

    public RecordServiceStructField(final String fieldName,
        final ObjectInspector inspector, final int fieldIndex) {
      fieldName_ = fieldName;
      inspector_ = inspector;
      fieldIdx_ = fieldIndex;
    }

    public int getProjectedIdx() { return projectedColIdx_; }

    /**
     * Sets the projected column index. Can only be called once.
     */
    public void setProjectedColIdx(int projectedColIdx) {
      Preconditions.checkState(!isProjectedColIdxSet_);
      isProjectedColIdxSet_ = true;
      projectedColIdx_ = projectedColIdx;
    }

    public boolean isProjectedColIdxSet() { return isProjectedColIdxSet_; }

    /**
     * Returns true if this column is selected as part of the projection
     * or if it is unknown whether the column is projected (setProjectedColIdx
     * has not been called).
     */
    public boolean isProjected() {
      return !isProjectedColIdxSet_ || projectedColIdx_ >= 0;
    }

    @Override
    public String getFieldComment() { return ""; }

    @Override
    public String getFieldName() { return fieldName_; }

    @Override
    public ObjectInspector getFieldObjectInspector() { return inspector_; }

    @Override
    public int getFieldID() { return fieldIdx_; }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      List<RecordServiceStructField> other = ((RecordServiceObjectInspector) o).fields_;
      if (other.size() != fields_.size()) {
        return false;
      }
      for(int i = 0; i < fields_.size(); ++i) {
        StructField left = other.get(i);
        StructField right = fields_.get(i);
        if (!(left.getFieldName().equals(right.getFieldName()) &&
              left.getFieldObjectInspector().equals
                  (right.getFieldObjectInspector()))) {
          return false;
        }
      }
      return true;
    }
  }
}

