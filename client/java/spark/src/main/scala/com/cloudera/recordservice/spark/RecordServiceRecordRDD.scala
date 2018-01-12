/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.recordservice.spark

import com.cloudera.recordservice.core.Records

import org.apache.spark._

/**
 * RDD that is backed by the RecordService. This returns an RDD of the lowest level
 * Record object.
 */
class RecordServiceRecordRDD(@transient sc: SparkContext)
  extends RecordServiceRDDBase[Records.Record](sc) with Logging {

  override def setTable(table:String) = {
    super.setTable(table)
    this
  }

  override def setStatement(stmt:String) = {
    super.setStatement(stmt)
    this
  }

  override def setPath(path:String) = {
    super.setPath(path)
    this
  }

  /**
   * Executes the task against the RecordServiceWorker and returns an iterator to fetch
   * result for the entire task.
   */
  override def compute(split: Partition, context: TaskContext):
      InterruptibleIterator[Records.Record] = {
    val iter = new NextIterator[Records.Record] {
      val partition = split.asInstanceOf[RecordServicePartition]

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }

      var (worker, records) = execTask(partition)

      override def getNext() : Records.Record = {
        if (!records.hasNext()) {
          finished = true
          return null
        }
        records.next()
      }

      override def close() = {
        if (records != null) {
          updateCounters(records)
          records.close()
          records = null
        }
        if (worker != null) {
          worker.close()
          worker = null
        }
      }
    }

    new InterruptibleIterator[Records.Record](context, iter)
  }

  /**
   * Sends the request to the RecordServicePlanner to generate the list of partitions
   * (tasks in RecordService terminology)
   */
  override protected def getPartitions: Array[Partition] = {
    super.planRequest._2
  }
}
