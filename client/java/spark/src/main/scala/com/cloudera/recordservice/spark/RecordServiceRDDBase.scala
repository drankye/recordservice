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

import com.cloudera.recordservice.core._
import com.cloudera.recordservice.mr.{PlanUtil, WorkerUtil}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration

private class RecordServicePartition(rddId: Int, idx: Int,
                                     h: Seq[NetworkAddress],
                                     w: Seq[NetworkAddress],
                                     t: Task, s: Schema,
                                     token: DelegationToken)
  extends Partition {
  override def hashCode(): Int = 41 * (41 + rddId) + idx
  override val index: Int = idx
  val task: Task = t
  val schema: Schema = s
  val localHosts: Seq[NetworkAddress] = h
  val globalHosts: Seq[NetworkAddress] = w
  val delegationToken: DelegationToken = token
}

/**
 * RDD that is backed by the RecordService. This is the base class that handles some of
 * common Spark and RecordService interactions.
 *
 * Security: currently, if kerberos is enabled, the planner request will get a delegation
 * token that is stored in the partition object. The RDD authenticates with the worker
 * using this token.
 * TODO: is there a more proper way to do this in Spark? It doesn't look like SparkContext
 * has a way to do this.
 */
abstract class RecordServiceRDDBase[T:ClassTag](@transient sc: SparkContext)
    extends RDD[T](sc, Nil) with Logging {

  // Metrics from the RecordServiceServer
  val recordsReadAccum = sc.accumulator(0L, "RecordsRead")
  val recordsReturnedAccum = sc.accumulator(0L, "RecordsReturned")
  val serializeTimeAccum = sc.accumulator(0L, "SerializeTimeMs")
  val clientTimeAccum = sc.accumulator(0L, "ClientTimeMs")
  val decompressTimeAccum = sc.accumulator(0L, "DecompressTimeMs")
  val bytesReadAccum = sc.accumulator(0L, "BytesRead")
  val bytesReadLocalAccum = sc.accumulator(0L, "BytesReadLocal")

  // system env for container id
  private val CONTAINER_ID: String = "CONTAINER_ID"

  // application id for the spark job
  private var app_id: String = ""

  // Configs that we use when we execute the task. These come from SparkContext
  // but we do not serialize the entire context. Instead these are populated in
  // the client (i.e. planning phase).
  val rsConfigs = saveFromSparkContext(sc)

  // Request to make
  @transient var request:Request = null

  // Result schema (after projection)
  var schema:Schema = null

  def setStatement(stmt:String) = {
    verifySetRequest()
    request = Request.createSqlRequest(stmt)
    this
  }

  def setTable(table:String) = {
    verifySetRequest()
    request = Request.createTableScanRequest(table)
    this
  }

  def setPath(path:String) = {
    verifySetRequest()
    request = Request.createPathRequest(path)
    this
  }

  def setRequest(req:Request) = {
    verifySetRequest()
    request = req
    this
  }

  def getSchema(): Schema = {
    if (schema == null) {
      // TODO: this is kind of awkward. Introduce a new plan() API?
      throw new SparkException("getSchema() can only be called after getPartitions")
    }
    schema
  }

  protected def verifySetRequest() = {
    if (request != null) {
      throw new SparkException("Request is already set.")
    }
  }

  /**
   * Executes 'stmt' and returns the worker and Records object associated with it.
   */
  protected def execTask(partition: RecordServicePartition) = {
    var worker:RecordServiceWorkerClient = null
    var records:Records = null
    try {
      val conf = new Configuration
      rsConfigs foreach (e => conf.set(e._1, e._2))
      val builder = WorkerUtil.getBuilder(conf, partition.delegationToken)
      val addr = WorkerUtil.getWorkerToConnectTo(
        partition.task.taskId, partition.localHosts, partition.globalHosts)
      worker = builder.connect(addr.hostname, addr.port)
      setTag(partition.task)
      records = worker.execAndFetch(partition.task)
      (worker, records)
    } catch {
      case e:RecordServiceException => logError("Could not exec request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
    } finally {
      if (records == null && worker != null) worker.close()
    }
  }

  // Returns a simplified schema for 'schema'. The RecordService supports richer types
  // than Spark so collapse types.
  protected def simplifySchema(schema:Schema) : Array[Schema.Type] = {
    val result = new Array[Schema.Type](schema.cols.size())
    for (i <- 0 until schema.cols.size()) {
      val t = schema.cols.get(i).`type`.typeId
      if (t == Schema.Type.VARCHAR || t == Schema.Type.CHAR) {
        result(i) = Schema.Type.STRING
      } else {
        result(i) = t
      }
    }
    result
  }

  /**
   * Updates the counters (accumulators) from records. This is *not* idempotent
   * and can only be called once per task, at the end of the task.
   */
  protected def updateCounters(records:Records) = {
    val stats = records.getStatus.stats
    recordsReadAccum += stats.numRecordsRead
    recordsReturnedAccum += stats.numRecordsReturned
    serializeTimeAccum += stats.serializeTimeMs
    clientTimeAccum += stats.clientTimeMs
    decompressTimeAccum += stats.decompressTimeMs
    bytesReadAccum += stats.bytesRead
    bytesReadLocalAccum += stats.bytesReadLocal
  }

  /**
   * Returns the list of preferred hosts to run this partition.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val partition = split.asInstanceOf[RecordServicePartition]
    partition.localHosts.map(_.hostname)
  }

  /**
   * Plans the request for 'stmt'. Returns the plan request and the spark list of
   * partitions.
   */
  protected def planRequest = {
    if (request == null) {
      throw new SparkException(
          "Request not set. Must call setStatement(), setTable() or setPath()")
    }

    var planner: RecordServicePlannerClient = null
    val (planResult, delegationToken) = try {
      // TODO: for use cases like oozie, we need to authenticate with the planner via
      // delegationToken as well. How is this done for spark? How do we get at the
      // credentials object.
      val conf = RecordServiceConf.fromSparkContext(sc)
      val principal = PlanUtil.getKerberosPrincipal(conf)
      val builder = PlanUtil.getBuilder(conf)
      val hostPorts = PlanUtil.getPlannerHostPorts(conf)
      // If the login user is different from the current user, we set the current user as
      // the delegated user. Here the login user is the user to submit the spark job, and
      // the current user is the user to execute the spark job.
      if (!UserGroupInformation.getLoginUser.equals(
        UserGroupInformation.getCurrentUser)) {
        builder.setDelegatedUser(UserGroupInformation.getCurrentUser.getUserName)
      }

      planner = PlanUtil.getPlanner(
        sc.hadoopConfiguration, builder, hostPorts, principal, null)
      val result = planner.planRequest(request)
      if (principal != null) {
        (result, planner.getDelegationToken(""))
      } else {
        (result, null)
      }
    } catch {
      case e:RecordServiceException => logError("Could not plan request: " + e.message)
        throw new SparkException("RecordServiceRDD failed", e)
    } finally {
      if (planner != null) planner.close()
      if (sc.applicationId != null) app_id = sc.applicationId
    }

    val partitions = new Array[Partition](planResult.tasks.size())
    for (i <- 0 until planResult.tasks.size()) {
      partitions(i) = new RecordServicePartition(id, i,
        planResult.tasks.get(i).localHosts,
        planResult.hosts,
        planResult.tasks.get(i), planResult.schema, delegationToken)
    }
    schema = planResult.schema
    (planResult, partitions)
  }

  private def saveFromSparkContext(sc:SparkContext) : Map[String, String] = {
    val conf = RecordServiceConf.fromSparkContext(sc)
    conf map (e => (e.getKey, e.getValue)) toMap
  }

  /**
   * Set tag for the task.
   */
  private def setTag(task: Task) {
    var tag: String = System.getenv(CONTAINER_ID)
    if (tag == null || tag.isEmpty) {
      tag = "Spark-" + app_id
    }
    task.setTag(tag)
  }
}
