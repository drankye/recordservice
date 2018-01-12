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

import java.util.NoSuchElementException

import com.cloudera.recordservice.mr.RecordServiceConfig
import org.apache.hadoop.classification.{InterfaceAudience, InterfaceStability}
import org.apache.spark.{SparkConf, SparkContext}

import com.cloudera.recordservice.mr.RecordServiceConfig.ConfVars
import org.apache.spark.sql.SQLContext

import org.apache.hadoop.conf.Configuration

@InterfaceAudience.Public
@InterfaceStability.Evolving
object RecordServiceConf {
  // Spark requires that configs start with "spark." to be read.
  val SPARK_CONF_PREFIX = "spark."

  /**
   * Create a hadoop configuration and copy all RecordService related
   * properties from the SparkContext to it, striping the "spark." prefix.
   */
  def fromSparkContext(sc:SparkContext) : Configuration = {
    val conf = new Configuration
    ConfVars.values() foreach(v => {
      val sparkKey = SPARK_CONF_PREFIX + v.name
      sc.getConf.getOption(sparkKey) match {
        case Some(value) =>
          conf.set(v.name, value)
        case None =>
          // Do nothing
      }})
    conf
  }

  /**
   * Create a hadoop configuration and copy all RecordService related
   * properties from the SQLContext to it, striping the "spark." prefix.
   */
  def fromSQLContext(sc:SQLContext) : Configuration = {
    val conf = new Configuration
    ConfVars.values() foreach (v => {
      val sparkKey = SPARK_CONF_PREFIX + v.name
      try {
        conf.set(v.name, sc.getConf(sparkKey))
      } catch {
        case e:NoSuchElementException =>
         // Key doesn't exist in sc - do nothing
      }
    })
    conf
  }

  /**
   * Set key and value in the SparkConf 'conf', adding the 'spark.' prefix to the former.
   */
  def setSparkConf(conf:SparkConf, key:RecordServiceConfig.ConfVars,
                   value:String) : Unit = {
    conf.set(SPARK_CONF_PREFIX + key.name, value)
  }

  /**
   * Set key and value in the SparkContext 'sc', adding the "spark." prefix to the former.
   */
  def setSparkContext(sc:SparkContext, key:RecordServiceConfig.ConfVars,
                      value:String) : Unit = {
    setSparkConf(sc.getConf, key, value)
  }

  /**
   * Set key and value in the SQLContext 'sc', adding the "spark." prefix to the former.
   */
  def setSQLContext(sc:SQLContext, key:RecordServiceConfig.ConfVars,
                    value:String) : Unit = {
    sc.setConf(SPARK_CONF_PREFIX + key.name, value)
  }
}
