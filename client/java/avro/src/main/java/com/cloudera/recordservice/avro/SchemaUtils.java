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

package com.cloudera.recordservice.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.specific.SpecificData;

public class SchemaUtils {
  /**
   * Converts a RecordService schema to an avro schema.
   */
  public static Schema convertSchema(
      com.cloudera.recordservice.core.Schema schema) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    for (int i = 0; i < schema.cols.size(); ++i) {
      Schema fieldSchema;
      switch (schema.cols.get(i).type.typeId) {
        case BOOLEAN: fieldSchema = Schema.create(Type.BOOLEAN); break;
        case TINYINT:
        case SMALLINT:
        case INT: fieldSchema = Schema.create(Type.INT); break;
        case BIGINT: fieldSchema = Schema.create(Type.LONG); break;
        case FLOAT: fieldSchema = Schema.create(Type.FLOAT); break;
        case DOUBLE: fieldSchema = Schema.create(Type.DOUBLE); break;
        case STRING:
        case VARCHAR:
        case CHAR:
          fieldSchema = Schema.create(Type.STRING);
          break;
        default:
          throw new RuntimeException(
              "Unsupported type: " + schema.cols.get(i).type.typeId);
      }
      fields.add(new Schema.Field(schema.cols.get(i).name, fieldSchema, "", null));
    }
    return Schema.createRecord(fields);
  }

  /**
   * Check if it is Specific Record according to the input schema from job config.
   * Return true if the input schema is not null and there is a specific class for it.
   */
  public static boolean isSpecificRecordSchema(Schema inputSchema) {
    return inputSchema != null && new SpecificData().getClass(inputSchema) != null;
  }
}
