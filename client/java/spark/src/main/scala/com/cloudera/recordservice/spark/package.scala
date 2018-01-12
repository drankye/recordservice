// Copyright 2012 Cloudera Inc.
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

package com.cloudera.recordservice

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package object spark {
  /**
   * Adds a method 'recordServiceRecords' to SparkContext to allow reading data
   * from the RecordService
   */
  implicit class RecordServiceContext(ctx: SparkContext) {
    def recordServiceRecords(sql: String) : RDD[Array[Writable]] = {
      new RecordServiceRDD(ctx).setStatement(sql)
    }

    def recordServiceTable(db: String, tbl:String) : RDD[Array[Writable]] = {
      new RecordServiceRDD(ctx).setTable(db + "." + tbl)
    }

    def recordServiceTextFile(path: String) : RDD[String] = {
      new RecordServiceRDD(ctx).setPath(path)
          .map(v => v(0).asInstanceOf[Text].toString)
    }
  }
}
