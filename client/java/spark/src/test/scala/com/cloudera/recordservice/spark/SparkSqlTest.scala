// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

package com.cloudera.recordservice.spark

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite}

// TODO: more tests
//  Add NULLS, error tests, filter tests
class SparkSqlTest extends FunSuite with SharedSparkSQLContext {
  test("Nation Test") {
    sc.sql(s"""
        |CREATE TEMPORARY TABLE nationTbl
        |USING com.cloudera.recordservice.spark.DefaultSource
        |OPTIONS (
        |  RecordServiceTable 'tpch.nation'
        |)
      """.stripMargin)

    // Try a count(*)
    assert (sc.sql("SELECT count(*) from nationTbl").collect()(0).getLong(0) == 25)
    assert (sc.sql("SELECT count(*) from nationTbl where n_nationkey = 1").
        collect()(0).getLong(0) == 1)

    // Scan the whole table
    var row = sc.sql("SELECT * from nationTbl").first()
    assert(row.get(0) == 0)
    assert(row.get(1) == "ALGERIA")
    assert(row.get(2) == 0)
    assert(row.get(3) == " haggle. carefully final deposits detect slyly agai")

    // Project two columns
    row = sc.sql("SELECT n_comment, n_name from nationTbl").collect()(5)
    assert(row.get(0) == "ven packages wake quickly. regu")
    assert(row.get(1) == "ETHIOPIA")
  }

  test("All Types Test") {
    sc.sql(s"""
              |CREATE TEMPORARY TABLE alltypestbl
              |USING com.cloudera.recordservice.spark.DefaultSource
              |OPTIONS (
              |  RecordServiceTable 'rs.alltypes'
              |)
      """.stripMargin)

    val rows = sc.sql("SELECT * FROM alltypestbl").collect()
    assert(rows.length == 2)
    val schema = rows(0).schema
    assert(schema(0).dataType == BooleanType)
    assert(schema(1).dataType == IntegerType)
    assert(schema(2).dataType == IntegerType)
    assert(schema(3).dataType == IntegerType)
    assert(schema(4).dataType == LongType)
    assert(schema(5).dataType == FloatType)
    assert(schema(6).dataType == DoubleType)
    assert(schema(7).dataType == StringType)
    assert(schema(8).dataType == StringType)
    assert(schema(9).dataType == StringType)
    assert(schema(10).dataType == TimestampType)
    assert(schema(11).dataType.isInstanceOf[DecimalType])

    val formatter:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    formatter.setTimeZone(TimeZone.getTimeZone("GMT"))

    if (rows(0).getBoolean(0)) {
      assert(rows(0).getInt(1) == 0)
      assert(rows(0).getInt(2) == 1)
      assert(rows(0).getInt(3) == 2)
      assert(rows(0).getLong(4) == 3)
      assert(rows(0).getFloat(5) == 4.0f)
      assert(rows(0).getDouble(6) == 5.0)
      assert(rows(0).getString(7) == "hello")
      assert(rows(0).getString(8) == "vchar1")
      assert(rows(0).getString(9) == "char1")
      assert(formatter.format(rows(0).getTimestamp(10)) == "2015-01-01")
      assert(rows(0).getDecimal(11).toString == "3.1415920000")
    } else {
      assert(rows(0).getInt(1) == 6)
      assert(rows(0).getInt(2) == 7)
      assert(rows(0).getInt(3) == 8)
      assert(rows(0).getLong(4) == 9)
      assert(rows(0).getFloat(5) == 10.0f)
      assert(rows(0).getDouble(6) == 11.0)
      assert(rows(0).getString(7) == "world")
      assert(rows(0).getString(8) == "vchar2")
      assert(rows(0).getString(9) == "char2")
      assert(formatter.format(rows(0).getTimestamp(10)) == "2016-01-01")
      assert(rows(0).getDecimal(11).toString == "1234.5678900000")
    }
  }

  test("Predicate pushdown") {
    sc.sql( s"""
      |CREATE TEMPORARY TABLE nationTbl
      |USING com.cloudera.recordservice.spark.DefaultSource
      |OPTIONS (
      |  RecordServiceTable 'tpch.nation'
      |)
    """.stripMargin)

    var row:Row = null

    row = sc.sql("SELECT count(*) from nationTbl where n_nationkey > 10").collect()(0)
    assert(row.get(0) == 14)

    row = sc.sql(
      "SELECT count(*) from nationTbl where n_nationkey = 10 OR n_nationkey = 1")
      .collect()(0)
    assert(row.get(0) == 2)

    row = sc.sql(
      "SELECT count(*) from nationTbl where n_nationkey = 10 AND n_nationkey = 1")
      .collect()(0)
    assert(row.get(0) == 0)
  }

  test("DataFrame Test") {
    val df = sc.load("tpch.nation", "com.cloudera.recordservice.spark.DefaultSource")
    // SELECT n_regionkey, count(*) FROM tpch.nation GROUP BY 1 ORDER BY 1
    val result = df.groupBy("n_regionkey").count().orderBy("n_regionkey").collect()
    assert(result.length == 5)
    assert(result(0).toString() == "[0,5]")
    assert(result(1).toString() == "[1,5]")
    assert(result(2).toString() == "[2,5]")
    assert(result(3).toString() == "[3,5]")
    assert(result(4).toString() == "[4,5]")
  }
}
