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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}
import org.apache.hadoop.util.PureJavaCrc32
import org.apache.spark._

import com.cloudera.recordservice.mr.RecordServiceConfig
import com.cloudera.recordservice.examples.terasort._

/**
 * A Spark version of {@link com.cloudera.recordservice.examples.terasort.TeraChecksum}
 * To run this example, do:
 *
 * <pre>
 * {@code
 *   spark-submit \
 *     --class com.cloudera.recordservice.examples.spark.TeraChecksum \
 *     --master <master-url> \
 *     /path/to/recordservice-examples-spark-<version>.jar \
 *     <arguments>
 * }
 * </pre>
 *
 * You may also want to set some other Spark configuration properties for the job,
 * for instance, "num-executors", "executor-memory", etc.
 */
object TeraChecksum {
  def main(args: Array[String]): Unit = {
    if (args.length != 1 && args.length != 2) {
      Console.err.println(
        """Usage: TeraChecksum <input-data> [boolean:use-recordservice]
             <input-data> should be a HDFS path if use-recordservice is false,
             or a Hive table name if use-recordservice is true.""")
      System.exit(1)
    }

    // Set up app name, initialize SparkContext, etc.
    val sparkConf = new SparkConf().setAppName("TeraChecksum")
    val sc = new SparkContext(sparkConf)
    val conf = new JobConf

    // Determine whether we are using RecordService.
    val useRS = if (args.length == 2) args(1).toBoolean else false

    // If we are using RecordService, set input table name to be the first arg,
    // otherwise, set the input path to be it.
    if (useRS) RecordServiceConfig.setInputTable(conf, null, args(0))
    else FileInputFormat.setInputPaths(conf, new Path(args(0)))

    // Choose different input format depending on whether we are using RecordService.
    val inputFormatClass =
      if (useRS) classOf[RecordServiceTeraInputFormat]
      else classOf[TeraInputFormat]

    // Construct a RDD with the given input format
    val rdd = sc.newAPIHadoopRDD(conf, inputFormatClass, classOf[Text], classOf[Text])

    // The main logic for the TeraChecksum app. This first maps each key-value pair to
    // a checksum value, then aggregates the results by combining pairs with "add".
    val zero = new Unsigned16
    val result = rdd.map {
      case (key, value) =>
        val crc32 = new PureJavaCrc32
        val checksum = new Unsigned16
        crc32.update(key.getBytes, 0, key.getLength)
        crc32.update(value.getBytes, 0, value.getLength)
        checksum.set(crc32.getValue)
        checksum
    }.fold(zero)((a, b) => { a.add(b); a })

    // Finally print the result and stop the SparkContext.
    println("Result: " + result)
    sc.stop()
  }
}
