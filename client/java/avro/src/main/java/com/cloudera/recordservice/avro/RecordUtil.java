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

package com.cloudera.recordservice.avro;

import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;

import com.cloudera.recordservice.core.Records;

/**
 * Util class for SpecificRecords and KeyValueRecords.
 *
 * TODO: map STRING to BYTES?
 */
public class RecordUtil {

  public enum ResolveBy {
    ORDINAL,
    NAME,
  }

  /**
   * Verifies and resolves the record schema with the RecordService schema.
   * Return an array of rsIndex mapping from record index. Set -1 as rsIndex only when
   * using the column's default value in reader schema.
   */
  public static int[] resolveSchema(org.apache.avro.Schema recordSchema,
      com.cloudera.recordservice.core.Schema rsSchema, ResolveBy resolveBy) {
    if (recordSchema.getType() != Schema.Type.RECORD) {
      throw new RuntimeException(
          "Incompatible schema: generic type must be a RECORD.");
    }

    List<Schema.Field> recordFields = recordSchema.getFields();
    List<com.cloudera.recordservice.core.Schema.ColumnDesc> rsSchemaCols = rsSchema.cols;
    int[] recordIndexToRsIndex = new int[recordFields.size()];

    if (resolveBy == ResolveBy.ORDINAL) {
      for (int i = 0; i < recordFields.size(); ++i) {
        RecordUtil.resolveType(recordFields.get(i).schema(), i,
            rsSchemaCols.get(i).type.typeId);
        recordIndexToRsIndex[i] = i;
      }
    } else if (resolveBy == ResolveBy.NAME) {
      HashMap<String, Integer> rsFields = new HashMap<String, Integer>();
      for (int i = 0; i < rsSchemaCols.size(); ++i) {
        // TODO: case sensitive?
        rsFields.put(rsSchemaCols.get(i).name.toLowerCase(), i);
      }

      for (int i = 0; i < recordFields.size(); ++i) {
        String fieldName = recordFields.get(i).name();
        if (!rsFields.containsKey(fieldName)) {
          // Field missing in writer schema, check if the field is nullable or contains a
          // default value.
          if (recordFields.get(i).defaultValue() != null ||
              recordFields.get(i).schema().getType() == Schema.Type.UNION) {
            recordIndexToRsIndex[i] = -1;
          } else {
            throw new RuntimeException("Default value is not set");
          }

          continue;
        }
        int rsFieldIndex = rsFields.get(fieldName);
        RecordUtil.resolveType(recordFields.get(i).schema(), i,
            rsSchemaCols.get(rsFieldIndex).type.typeId);
        recordIndexToRsIndex[i] = rsFieldIndex;
      }
    } else {
      throw new RuntimeException("Not implemented");
    }
    return recordIndexToRsIndex;
  }

  /**
   * Build record via adding columns. Use the col value of 'rsRecord' if it is not null,
   * otherwise use the default col value from reader schema.
   */
  public static void buildRecord(com.cloudera.recordservice.core.Schema rsSchema,
      Records.Record rsRecord, org.apache.avro.Schema readerSchema,
      SpecificRecordBase record, int[] rsIndexToRecordIndex) {
    for (int i = 0; i < rsIndexToRecordIndex.length; ++i) {
      int rsIndex = rsIndexToRecordIndex[i];
      if (rsIndex == -1 || rsRecord.isNull(rsIndex)) {
        // Use col default value in reader schema.
        if (readerSchema.getFields().get(i).defaultValue() != null) {
          Schema.Type recordType = RecordUtil.getSchemaType(
              readerSchema.getFields().get(i).schema());

          switch (recordType) {
            case BOOLEAN:
              record.put(i,
                  readerSchema.getFields().get(i).defaultValue().getBooleanValue());
              break;
            case INT:
              record.put(i,
                  readerSchema.getFields().get(i).defaultValue().getIntValue());
              break;
            case LONG:
              record.put(i,
                  readerSchema.getFields().get(i).defaultValue().getLongValue());
              break;
            case FLOAT:
            case DOUBLE:
              record.put(i,
                  readerSchema.getFields().get(i).defaultValue().getDoubleValue());
              break;
            case STRING:
              record.put(i,
                  readerSchema.getFields().get(i).defaultValue().getTextValue());
              break;
            default:
              throw new RuntimeException("Unsupported type: " + recordType);
          }
        } else if (readerSchema.getFields().get(i).schema().getType() ==
            Schema.Type.UNION) {
          // For UNION schema type with null default value,
          // just use null as column value of the record.
          record.put(i, readerSchema.getFields().get(i).defaultValue());
        } else {
          throw new RuntimeException("Default value is not set");
        }
      } else {
        // Use col value from rsRecord.
        switch (rsSchema.cols.get(rsIndex).type.typeId) {
          case BOOLEAN:
            record.put(i, rsRecord.nextBoolean(rsIndex));
            break;
          case TINYINT:
            record.put(i, (int) rsRecord.nextByte(rsIndex));
            break;
          case SMALLINT:
            record.put(i, (int) rsRecord.nextShort(rsIndex));
            break;
          case INT:
            record.put(i, rsRecord.nextInt(rsIndex));
            break;
          case BIGINT:
            record.put(i, rsRecord.nextLong(rsIndex));
            break;
          case FLOAT:
            record.put(i, rsRecord.nextFloat(rsIndex));
            break;
          case DOUBLE:
            record.put(i, rsRecord.nextDouble(rsIndex));
            break;

          case STRING:
          case VARCHAR:
          case CHAR:
            record.put(i, rsRecord.nextByteArray(rsIndex).toString());
            break;

          default:
            throw new RuntimeException(
                "Unsupported type: " + rsSchema.cols.get(rsIndex).type);
        }
      }
    }
  }

  /**
   * Verifies that the type of reader schema is compatible with the type of writer
   * schema - 'rsType'.
   */
  private static void resolveType(Schema readerSchema, int readerIndex,
                                  com.cloudera.recordservice.core.Schema.Type rsType) {
    // TODO: support avro's schema resolution rules with type promotion.
    Schema.Type recordType = getSchemaType(readerSchema);

    switch (recordType) {
      case ARRAY:
      case MAP:
      case RECORD:
      case UNION:
        throw new RuntimeException("Nested schemas are not supported");

      case ENUM:
      case NULL:
      case BYTES:
      case FIXED:
        // TODO: Support those above types
        throw new RuntimeException("Annotated types are not supported");

      case BOOLEAN:
        if (rsType == com.cloudera.recordservice.core.Schema.Type.BOOLEAN) return;
        break;
      case INT:
        if (rsType == com.cloudera.recordservice.core.Schema.Type.TINYINT ||
            rsType == com.cloudera.recordservice.core.Schema.Type.SMALLINT ||
            rsType == com.cloudera.recordservice.core.Schema.Type.INT) {
          return;
        }
        break;
      case LONG:
        if (rsType == com.cloudera.recordservice.core.Schema.Type.BIGINT) return;
        break;
      case FLOAT:
        if (rsType == com.cloudera.recordservice.core.Schema.Type.FLOAT) return;
        break;
      case DOUBLE:
        if (rsType == com.cloudera.recordservice.core.Schema.Type.DOUBLE) return;
        break;
      case STRING:
        if (rsType == com.cloudera.recordservice.core.Schema.Type.STRING) return;
        break;
      default:
        throw new RuntimeException("Unsupported type: " + recordType);
    }

    throw new RuntimeException("Field at position " + readerIndex +
        " have incompatible types. RecordService returned " + rsType +
        ". Record expects " + recordType);
  }

  /**
   * Return the schema type, if it is union type, will return the not null type.
   */
  private static Schema.Type getSchemaType(Schema schema) {
    Schema.Type recordType = schema.getType();

    if (recordType == Schema.Type.UNION) {
      // Unions are special because they are used to support NULLable types. In this
      // case the union has two types, one of which is NULL.
      List<Schema> children = schema.getTypes();
      if (children.size() != 2) {
        throw new RuntimeException("Only union schemas with NULL are supported.");
      }
      Schema.Type t1 = children.get(0).getType();
      Schema.Type t2 = children.get(1).getType();
      if (t1 != Schema.Type.NULL && t2 != Schema.Type.NULL) {
        throw new RuntimeException("Only union schemas with NULL are supported.");
      }

      if (t1 == Schema.Type.NULL) recordType = t2;
      if (t2 == Schema.Type.NULL) recordType = t1;
    }
    return recordType;
  }
}
