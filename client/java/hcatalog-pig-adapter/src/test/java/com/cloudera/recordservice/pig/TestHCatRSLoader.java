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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.junit.Test;

// TODO: add more tests
public class TestHCatRSLoader extends TestBase {
  private static double DELTA = 0.0001;

  @Test
  public void testNation() throws IOException {
    server.registerQuery("A = LOAD " +
        "'tpch.nation' USING com.cloudera.recordservice.pig.HCatRSLoader();");

    Schema s = server.dumpSchema("A");
    List<Schema.FieldSchema> fields = s.getFields();
    assertEquals(4, fields.size());
    assertEquals("n_nationkey", fields.get(0).alias);
    assertEquals(DataType.INTEGER, fields.get(0).type);
    assertEquals("n_name", fields.get(1).alias);
    assertEquals(DataType.CHARARRAY, fields.get(1).type);
    assertEquals("n_regionkey", fields.get(2).alias);
    assertEquals(DataType.INTEGER, fields.get(2).type);
    assertEquals("n_comment", fields.get(3).alias);
    assertEquals(DataType.CHARARRAY, fields.get(3).type);

    server.registerQuery("A_GROUP = GROUP A ALL;");
    server.registerQuery("A_COUNT = FOREACH A_GROUP GENERATE COUNT(A);");
    Iterator<Tuple> it = server.openIterator("A_COUNT");
    boolean first = true;
    while (it.hasNext()) {
      assertTrue("Result should only contain one row", first);
      first = false;
      Tuple t = it.next();
      assertEquals(DataType.LONG, t.getType(0));
      assertEquals(1, t.size());
      assertEquals(25L, t.get(0));
    }
  }

  @Test
  public void testNationSelect() throws IOException {
    server.registerQuery("A = LOAD 'select n_name, n_comment from tpch.nation'" +
        " USING com.cloudera.recordservice.pig.HCatRSLoader();");
    Schema s = server.dumpSchema("A");
    // TODO: note: there's a bug in HCatRSLoader#getSchema, which returns the
    // schema for the whole table instead of the selected columns. Fix the test
    // after that is resolved.
    List<Schema.FieldSchema> fields = s.getFields();
    assertEquals(4, fields.size());
    assertEquals("n_nationkey", fields.get(0).alias);
    assertEquals(DataType.INTEGER, fields.get(0).type);
    assertEquals("n_name", fields.get(1).alias);
    assertEquals(DataType.CHARARRAY, fields.get(1).type);
    assertEquals("n_regionkey", fields.get(2).alias);
    assertEquals(DataType.INTEGER, fields.get(2).type);
    assertEquals("n_comment", fields.get(3).alias);
    assertEquals(DataType.CHARARRAY, fields.get(3).type);

    Iterator<Tuple> it = server.openIterator("A");
    int count = 0;
    while (it.hasNext()) {
      count++;
      Tuple t = it.next();
      assertEquals(2, t.size());
      assertEquals(DataType.CHARARRAY, t.getType(0));
      assertEquals(DataType.CHARARRAY, t.getType(1));
      if (count == 1) {
        assertEquals("ALGERIA", t.get(0));
        assertEquals(" haggle. carefully final deposits detect slyly agai", t.get(1));
      }
    }
    assertEquals(25L, count);
  }

  @Test
  public void testAllTypes() throws IOException {
    server.registerQuery("A = LOAD 'rs.alltypes'" +
        " USING com.cloudera.recordservice.pig.HCatRSLoader();");
    Schema s = server.dumpSchema("A");
    List<Schema.FieldSchema> fields = s.getFields();
    assertEquals(12, fields.size());
    assertEquals("bool_col", fields.get(0).alias);
    assertEquals(DataType.BOOLEAN, fields.get(0).type);
    assertEquals("tinyint_col", fields.get(1).alias);
    assertEquals(DataType.INTEGER, fields.get(1).type);
    assertEquals("smallint_col", fields.get(2).alias);
    assertEquals(DataType.INTEGER, fields.get(2).type);
    assertEquals("int_col", fields.get(3).alias);
    assertEquals(DataType.INTEGER, fields.get(3).type);
    assertEquals("bigint_col", fields.get(4).alias);
    assertEquals(DataType.LONG, fields.get(4).type);
    assertEquals("float_col", fields.get(5).alias);
    assertEquals(DataType.FLOAT, fields.get(5).type);
    assertEquals("double_col", fields.get(6).alias);
    assertEquals(DataType.DOUBLE, fields.get(6).type);
    assertEquals("string_col", fields.get(7).alias);
    assertEquals(DataType.CHARARRAY, fields.get(7).type);
    assertEquals("varchar_col", fields.get(8).alias);
    assertEquals(DataType.CHARARRAY, fields.get(8).type);
    assertEquals("char_col", fields.get(9).alias);
    assertEquals(DataType.CHARARRAY, fields.get(9).type);
    assertEquals("timestamp_col", fields.get(10).alias);
    assertEquals(DataType.DATETIME, fields.get(10).type);
    assertEquals("decimal_col", fields.get(11).alias);
    assertEquals(DataType.BIGDECIMAL, fields.get(11).type);

    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

    Iterator<Tuple> it = server.openIterator("A");
    int count = 0;
    while (it.hasNext()) {
      count++;
      Tuple t = it.next();
      assertEquals(12, t.size());
      if ((Boolean) t.get(0)) {
        assertEquals(0, ((Byte) t.get(1)).intValue());
        assertEquals(1, ((Integer) t.get(2)).intValue());
        assertEquals(2, ((Integer) t.get(3)).intValue());
        assertEquals(3, ((Long) t.get(4)).intValue());
        assertEquals(4.0f, ((Float) t.get(5)).intValue(), DELTA);
        assertEquals(5.0, ((Double) t.get(6)).intValue(), DELTA);
        assertEquals("hello", t.get(7));
        assertEquals("vchar1", t.get(8));
        assertEquals("char1", t.get(9));
        Timestamp ts = new Timestamp(((DateTime) t.get(10)).getMillis());
        assertEquals("2015-01-01", formatter.format(ts));
        assertEquals("3.1415920000", t.get(11).toString());
      } else {
        assertEquals(6, ((Byte) t.get(1)).intValue());
        assertEquals(7, ((Integer) t.get(2)).intValue());
        assertEquals(8, ((Integer) t.get(3)).intValue());
        assertEquals(9, ((Long) t.get(4)).intValue());
        assertEquals(10.0f, ((Float) t.get(5)).intValue(), DELTA);
        assertEquals(11.0, ((Double) t.get(6)).intValue(), DELTA);
        assertEquals("world", t.get(7));
        assertEquals("vchar2", t.get(8));
        assertEquals("char2", t.get(9));
        Timestamp ts = new Timestamp(((DateTime) t.get(10)).getMillis());
        assertEquals("2016-01-01", formatter.format(ts));
        assertEquals("1234.5678900000", t.get(11).toString());
      }
    }
    assertEquals(2, count);
  }

}
