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

package com.cloudera.recordservice.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.recordservice.thrift.TColumnDesc;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TType;
import com.cloudera.recordservice.thrift.TTypeId;

/**
 * POJO for TSchema and related thrift structs.
 */
public class Schema implements Serializable {
  private static final long serialVersionUID = 4583157059155162786L;

  public static enum Type implements Serializable {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    STRING,
    VARCHAR,
    CHAR,
    TIMESTAMP_NANOS,
    DECIMAL;

    static Type fromThrift(TTypeId id) {
      switch (id) {
      case BOOLEAN: return BOOLEAN;
      case TINYINT: return TINYINT;
      case SMALLINT: return SMALLINT;
      case INT: return INT;
      case BIGINT: return BIGINT;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      case STRING: return STRING;
      case VARCHAR: return VARCHAR;
      case CHAR: return CHAR;
      case TIMESTAMP_NANOS: return TIMESTAMP_NANOS;
      case DECIMAL: return DECIMAL;
      }
      throw new RuntimeException("Unsupported type: " + id);
    }
  }

  public static final class TypeDesc implements Serializable {
    private static final long serialVersionUID = 2810731415887109906L;

    public final Type typeId;
    public final int precision;
    public final int scale;
    public final int len;

    public TypeDesc(TType t) {
      typeId = Type.fromThrift(t.type_id);
      precision = t.precision;
      scale = t.scale;
      len = t.len;
    }

    public TypeDesc(Type t) {
      this(t, 0, 0, 0);
      if (t == Type.VARCHAR || t == Type.CHAR || t == Type.DECIMAL) {
        throw new IllegalArgumentException(
            "Cannot construct parameterized type with this constructor.");
      }
    }

    @Override
    public String toString() {
      if (typeId == Type.VARCHAR || typeId == Type.CHAR) {
        return typeId.name() + "(" + len + ")";
      } else if (typeId == Type.DECIMAL) {
        return typeId.name() + "(" + precision + ", " + scale + ")";
      } else {
        return typeId.name();
      }
    }

    public static TypeDesc createDecimalType(int precision, int scale) {
      return new TypeDesc(Type.DECIMAL, 0, precision, scale);
    }

    public static TypeDesc createVarCharType(int len) {
      return new TypeDesc(Type.VARCHAR, len, 0, 0);
    }

    public static TypeDesc createCharType(int len) {
      return new TypeDesc(Type.CHAR, len, 0, 0);
    }

    TType toThrift() {
      TType t = new TType();
      switch (typeId) {
        case BOOLEAN: t.type_id = TTypeId.BOOLEAN; break;
        case TINYINT: t.type_id = TTypeId.TINYINT; break;
        case SMALLINT: t.type_id = TTypeId.SMALLINT; break;
        case INT: t.type_id = TTypeId.INT; break;
        case BIGINT: t.type_id = TTypeId.BIGINT; break;
        case FLOAT: t.type_id = TTypeId.FLOAT; break;
        case DOUBLE: t.type_id = TTypeId.DOUBLE; break;
        case STRING: t.type_id = TTypeId.STRING; break;
        case VARCHAR:
          t.type_id = TTypeId.VARCHAR;
          t.setLen(len);
          break;
        case CHAR:
          t.type_id = TTypeId.CHAR;
          t.setLen(len);
          break;
        case TIMESTAMP_NANOS: t.type_id = TTypeId.TIMESTAMP_NANOS; break;
        case DECIMAL:
          t.type_id = TTypeId.DECIMAL;
          t.setPrecision(precision);
          t.setScale(scale);
          break;
      }
      return t;
    }

    TypeDesc(Type t, int len, int precision, int scale) {
      this.typeId = t;
      this.len = len;
      this.precision = precision;
      this.scale = scale;
    }
  }

  public static final class ColumnDesc implements Serializable {
    private static final long serialVersionUID = 7003243606324360627L;
    public final String name;
    public final TypeDesc type;

    public ColumnDesc(String n, TypeDesc t) {
      name = n;
      type = t;
    }

    @Override
    public String toString() {
      return name + ": " + type;
    }

    ColumnDesc(TColumnDesc desc) {
      name = desc.name;
      type = new TypeDesc(desc.type);
    }
  }

  public final List<ColumnDesc> cols;
  public final boolean isCountStar;

  public Schema() {
    cols = new ArrayList<ColumnDesc>();
    isCountStar = false;
  }

  Schema(List<ColumnDesc> c, boolean isCountStar) {
    cols = c;
    this.isCountStar = isCountStar;
  }

  /**
   * Serializes this schema to 'out'
   */
  public void serialize(DataOutput out) throws IOException {
    out.writeInt(cols.size());
    for (ColumnDesc d: cols) {
      out.writeInt(d.name.length());
      out.writeBytes(d.name);
      out.writeInt(d.type.typeId.ordinal());
      out.writeInt(d.type.precision);
      out.writeInt(d.type.scale);
      out.writeInt(d.type.len);
    }
    out.writeBoolean(isCountStar);
  }

  /**
   * Deserializes Schema from 'in'
   */
  public static Schema deserialize(DataInput in) throws IOException {
    int numCols = in.readInt();
    List<ColumnDesc> cols = new ArrayList<ColumnDesc>();
    for (int i = 0; i < numCols; ++i) {
      int nameLen = in.readInt();
      byte[] nameBuffer = new byte[nameLen];
      in.readFully(nameBuffer);
      Type typeId = Type.values()[in.readInt()];
      int precision = in.readInt();
      int scale = in.readInt();
      int len = in.readInt();
      TypeDesc t = new TypeDesc(typeId, len, precision, scale);
      cols.add(new ColumnDesc(new String(nameBuffer), t));
    }
    boolean isCountStart = in.readBoolean();
    return new Schema(cols, isCountStart);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Schema:");
    for (ColumnDesc c: cols) {
      sb.append("\n  ").append(c);
    }
    return sb.toString();
  }

  Schema(TSchema schema) {
    cols = new ArrayList<ColumnDesc>();
    for (int i = 0; i < schema.cols.size(); ++i) {
      cols.add(new ColumnDesc(schema.cols.get(i)));
    }
    isCountStar = schema.is_count_star;
  }

  TSchema toThrift() {
    TSchema schema = new TSchema();
    schema.cols = new ArrayList<TColumnDesc>();
    for (int i = 0; i < cols.size(); ++i) {
      schema.addToCols(new TColumnDesc(cols.get(i).type.toThrift(), cols.get(i).name));
    }
    schema.is_count_star = isCountStar;
    return schema;
  }
}
