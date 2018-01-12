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

package com.cloudera.recordservice.examples.spark

import com.cloudera.recordservice.spark.SchemaRecordServiceRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Example that uses the SchemaRecordService RDD
 */
// First declare your "row object" as a case class. This must match (with some resolution
// rules) the schema of the result from the RecordService.

// In this case we are matching by ordinal so the types of the fields must be a prefix
// of the table/query
case class NationByOrdinal(var key:Short, var name:String, var regionKey:Short)

// In this case we are matching by name so the fields must be a subset of the table/query
case class NationByName(var n_name:String)

case class AllTypesProjection(var int_col:Int, var string_col:String)

object SchemaRDDExample {

  // In this example we match by ordinal. The N fields in the case class will be populated
  // by the first N fields of the result.
  def nationByOrdinalExample(sc:SparkContext) = {
    val data = new SchemaRecordServiceRDD[NationByOrdinal](
      sc, classOf[NationByOrdinal], true).setTable("tpch.nation")
    data.foreach(n => println(n.key + ", " + n.name))
  }

  // In these two examples we match by name. The fields in the case class must match
  // the name of the schema returned by the underlying service.
  def nationByNameExample(sc:SparkContext) = {
    val data = new SchemaRecordServiceRDD[NationByName](
      sc, classOf[NationByName], false).setStatement("select * from tpch.nation")
    data.foreach(n => println(n.n_name))
  }

  def allTypesExample(sc:SparkContext) = {
    val data = new SchemaRecordServiceRDD[AllTypesProjection](
      sc, classOf[AllTypesProjection], false).setTable("rs.alltypes")
    data.foreach(n => println(n.int_col))
  }

  // In this example, the data contains a NULL in the projection. The object is then
  // populated with the default we provide.
  def allTypesDefaultExample(sc:SparkContext) = {
    val data = new SchemaRecordServiceRDD[AllTypesProjection](
        sc, classOf[AllTypesProjection], false)
      .setTable("rs.alltypes_null")
      // Here we set the default. Any time we see NULL in the first column, we populate
      // it with -1. Any time in the second column, we populate with "Empty"
      .setDefaultValue(new AllTypesProjection(-1, "Empty"))
    data.foreach(n => println(n.int_col + " " + n.string_col))
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
        .setAppName("SchemaRDD Example")
        .setMaster("local")
    val sc = new SparkContext(sparkConf)

    nationByOrdinalExample(sc)
    nationByNameExample(sc)
    allTypesExample(sc)
    allTypesDefaultExample(sc)

    sc.stop()
  }
}
