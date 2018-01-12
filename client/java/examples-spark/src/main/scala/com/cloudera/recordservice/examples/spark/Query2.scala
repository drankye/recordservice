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

import com.cloudera.recordservice.spark.RecordServiceRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

// sum(l_partkey) group by l_returnflag
object Query2 {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("Query2")
      .setMaster("local[8]")
    var query = "select l_partkey,l_returnflag from tpch_parquet.lineitem"
    if (args.length == 1) query = args(0)

    val sc = new SparkContext(sparkConf)
    val data = new RecordServiceRDD(sc).setStatement(query)
    val values = data.map(v =>
        (v(1).asInstanceOf[org.apache.hadoop.io.Text].toString(),
         v(0).asInstanceOf[org.apache.hadoop.io.LongWritable].get()))
    values.reduceByKey(_ + _).foreach(println)
    sc.stop()
  }
}
