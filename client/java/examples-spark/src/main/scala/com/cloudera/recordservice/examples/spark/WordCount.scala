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
import org.apache.spark._

/**
 * Example application that does word count on a directory.
 * This demonstrates how the APIs differ. recordServiceTextFile() should be a
 * drop in replacement for textFile()
 *
 * To run the program:
 * spark-submit \
 * --class com.cloudera.recordservice.examples.spark.WordCount \
 * --properties-file=/path/to/spark.conf \
 * recordservice-examples-spark-*.jar \
 * <input path>
 *
 * Append false in the command to run WordCount without RecordService
 */
object WordCount {
  def main(args: Array[String]) {
    var useRecordService = true
    if (args.length == 2) {
      useRecordService = java.lang.Boolean.parseBoolean(args(1))
    }

    val sparkConf = new SparkConf()
      .setAppName("Word Count")

    var path = "/test-warehouse/tpch.nation/*"
    if (args.length >= 1) path = args(0)

    val sc = new SparkContext(sparkConf)

    val file = if (useRecordService) sc.recordServiceTextFile(path) else sc.textFile(path)

    val words = file.flatMap(line => tokenize(line))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    wordCounts.foreach(println)
    sc.stop()
  }

  // Split a piece of text into individual words.
  private def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
