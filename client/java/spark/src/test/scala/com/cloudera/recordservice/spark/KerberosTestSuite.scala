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

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Outcome,BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import com.cloudera.recordservice.mr.RecordServiceConfig.ConfVars

// TODO: add error tests.
class KerberosTestSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  // Kerberos hosts (planners & workers), planner and planner principal for testing
  var kerberosHosts_ : List[String] = List()
  var plannerHost_ : String = ""
  var plannerPrincipal_ : String = ""

  val KERBEROS_HOSTS = "KERBEROS_HOSTS"

  override def withFixture(test: NoArgTest): Outcome = {
    assume(sc != null)
    test()
  }

  override def beforeAll() {
    super.beforeAll()

    if (System.getenv("HAS_KERBEROS_CREDENTIALS") == null ||
        !System.getenv("HAS_KERBEROS_CREDENTIALS").equalsIgnoreCase("true")) {
      return
    }

    if (System.getenv(KERBEROS_HOSTS) != null) {
      kerberosHosts_ = System.getenv(KERBEROS_HOSTS).split(":").toList
      if (kerberosHosts_.isEmpty) {
        println("Can't find any host from the input '" + KERBEROS_HOSTS + "':"
          + System.getenv(KERBEROS_HOSTS))
        return
      }

      println(KERBEROS_HOSTS + ": ")
      kerberosHosts_.foreach(println)

      plannerHost_ = kerberosHosts_(0)
      plannerPrincipal_ = getPrincipal("impala", plannerHost_)
      val conf = new SparkConf(false)
      RecordServiceConf.setSparkConf(
        conf, ConfVars.PLANNER_HOSTPORTS_CONF, plannerHost_ + ":12050")
      RecordServiceConf.setSparkConf(
        conf, ConfVars.KERBEROS_PRINCIPAL_CONF, plannerPrincipal_)
      _sc = new SparkContext("local", "test", conf)
    } else {
      println("To run Kerberos tests, you need to set" +
        " environment variable '" + KERBEROS_HOSTS
        + "' with a colon separated list of kerberoized hosts.")
    }
  }

  override def afterAll() {
    LocalSparkContext.stop(_sc)
    _sc = null
    super.afterAll()
  }

  def getPrincipal(primary: String, hostname: String) = {
    val firstDot = hostname.indexOf(".")
    primary + "/" + hostname + "@" + hostname.substring(firstDot + 1).toUpperCase
  }

  test("NationTest") {
    val rdd = new RecordServiceRecordRDD(sc).setTable("sample_07")
    assert(rdd.count() == 823)
  }
}
