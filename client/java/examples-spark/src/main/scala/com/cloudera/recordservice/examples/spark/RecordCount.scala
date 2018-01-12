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

import com.cloudera.recordservice.spark.{RecordServiceContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * This is the Spark version of
 * {@link com.cloudera.recordservice.examples.mapreduce.RecordCount}. It just counts the
 * number of rows from results returned by executing the input query.
 *
 * To run this example, do:
 *
 * <pre>
 * {@code
 *   spark-submit \
 *     --class com.cloudera.recordservice.examples.spark.RecordCount \
 *     --master <master-url> \
 *     /path/to/recordservice-examples-spark-<version>.jar
 *     <query>
 * }
 *
 * </pre>
 *
 * You may also want to set some other Spark configuration properties for the job,
 * for instance, "num-executors", "executor-memory", etc.
 */
object RecordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      Console.err.println("Usage: RecordCount <query>")
      System.exit(1)
    }
    val query = args(0)

    // Set up app name, initialize SparkContext, etc.
    val sparkConf = new SparkConf().setAppName("RecordCount")
    val sc = new SparkContext(sparkConf)

    // Execute the input query using RecordService and count the number of rows
    // in the result data set.
    val rdd = sc.recordServiceRecords(query)
    val count = rdd.map(_ => 1L).reduce(_ + _)

    // Print the result and stop the SparkContext
    println("Result: " + count)
    sc.stop()
  }
}
