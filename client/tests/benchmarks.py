#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This contains descriptions of all the benchmarks. They are broken up into
# suites.

import os

IMPALA_SHELL_CMD = os.environ['IMPALA_HOME'] + "/bin/impala-shell.sh -i LOCALHOST -B "
PLANNER_HOST = os.environ['RECORD_SERVICE_PLANNER_HOST']

# TODO: right now this is used in several places (e.g., pom.xml). It would
# be good if there's a way to only define it at one place.
VERSION = "0.4.0-cdh5.8.x"

MR_TEMPLATE = "hadoop jar " + os.environ['RECORD_SERVICE_HOME'] +\
    "/java/examples/target/recordservice-examples-" + VERSION + ".jar " +\
    "{0} \"{1}\" \"{2}\" "

SPARK_TEMPLATE = "spark-submit " +\
    "--class {0} " +\
    "--master yarn-client " +\
    "--num-executors 16 " +\
    "--executor-memory 28g " +\
    "--conf \"spark.dynamicAllocation.enabled=true\" " +\
    "--driver-memory 20g " +\
    "--conf \"spark.driver.maxResultSize=15g\" " +\
    os.environ['RECORD_SERVICE_HOME'] +\
    "/java/examples-spark/target/recordservice-examples-spark-" + VERSION + ".jar " +\
    "\"{1}\" "

def read_file(path):
  data = ""
  with open(path, "r") as f:
    data += "\n" + f.read()
  return data

def impala_shell_cmd(query):
  return IMPALA_SHELL_CMD + "-q \"" + query + "\""

def impala_single_thread_cmd(query):
  query = "set num_scanner_threads=1;" + query
  return impala_shell_cmd(query)

def impala_on_rs_cmd(query):
  query = "set use_record_service=true;" + query
  return impala_shell_cmd(query)

def native_client_cmd(query):
  return os.environ['RECORD_SERVICE_HOME'] +\
      "/cpp/build/release/recordservice/record-service-client \"" +\
      query + "\""

def java_client_cmd(query):
  return "java -classpath " + os.environ['RECORD_SERVICE_HOME'] +\
      "/java/examples/target/recordservice-examples-" + VERSION + ".jar " +\
      "com.cloudera.recordservice.examples.SumQueryBenchmark " +\
      "\"" + query + "\""

def mr_record_count(query, outpath):
  return MR_TEMPLATE.format(
      "com.cloudera.recordservice.examples.mapreduce.RecordCount ", query, outpath)

def mr_word_count(inpath, outpath, use_rs="true"):
  return MR_TEMPLATE.format(
      "com.cloudera.recordservice.examples.mapreduce.WordCount ", inpath, outpath) + use_rs

def spark_record_count(query):
  return SPARK_TEMPLATE.format(
      "com.cloudera.recordservice.examples.spark.RecordCount", query)

def spark_word_count(inpath, use_rs="true"):
  return SPARK_TEMPLATE.format(
      "com.cloudera.recordservice.examples.spark.WordCount", inpath) + use_rs

def hive_rs_cmd(query, tbl_name, fetch_size):
  # Builds a query string that will run using the RecordService
  rs_query = """
      set hive.input.format=com.cloudera.recordservice.hive.RecordServiceHiveInputFormat;
      set recordservice.table.name={0};
      set recordservice.fetch.size={1};
      {2}""".format(tbl_name, fetch_size, query)
  return hive_cmd(rs_query)

def hive_cmd(query):
  return "hive -e \"" + query + "\""

#TODO: I think this spends a lot of time just starting up spark.
def spark_cmd(cl, query):
  return "java -classpath " + os.environ['RECORD_SERVICE_HOME'] +\
      "/java/examples-spark/target/recordservice-examples-spark-" + VERSION + ".jar " + cl +\
      " \"" + query + "\""

def spark_tpcds(query, use_rs=False):
    return "spark-submit " +\
    "--class com.cloudera.recordservice.examples.spark.TpcdsBenchmark " +\
    "--master yarn-client " +\
    "--num-executors 16 " +\
    "--executor-memory 28g " +\
    "--executor-cores 6 " +\
    "--driver-memory 20g " +\
    "--conf \"spark.driver.maxResultSize=15g\" " +\
    os.environ['RECORD_SERVICE_HOME'] +\
    "/java/examples-spark/target/recordservice-examples-spark-" + VERSION + ".jar " +\
    (PLANNER_HOST if use_rs else "SPARK") + " " + query

def spark_q1(query):
  return spark_cmd("com.cloudera.recordservice.examples.spark.Query1", query)

def spark_q2(query):
  return spark_cmd("com.cloudera.recordservice.examples.spark.Query2", query)

def impala_tpcds(query_name, record_service):
  path = os.environ['RECORD_SERVICE_HOME'] +\
      "/perf-queries/tpcds/tpcds-" + query_name + ".sql"
  query = read_file(path)
  query = "use tpcds500gb_parquet;\n" + query
  if record_service:
    query = "set use_record_service=true;\nset num_scanner_threads=32;" + query
  return impala_shell_cmd(query)

benchmarks = [
  [
    # Metadata about this suite. "local" indicates this benchmark should only be
    # run on a single node.
    # The first argument is used for file paths so cannot contain characters that
    # need to be escaped.
    "Query1_Text_6GB", "local",
    [
      # Each case to run. The first element is the name of the application and
      # the second is the shell command to run to run the benchmark
      ["impala", impala_shell_cmd("select sum(l_partkey) from tpch6gb.lineitem")],
      ["impala-single-thread", impala_single_thread_cmd(
          "select sum(l_partkey) from tpch6gb.lineitem")],
      ["impala-rs", impala_on_rs_cmd("select sum(l_partkey) from tpch6gb.lineitem")],
      ["native-client", native_client_cmd("select l_partkey from tpch6gb.lineitem")],
      ["java-client", java_client_cmd("select l_partkey from tpch6gb.lineitem")],
      ["spark-rs", spark_q1("select l_partkey from tpch6gb.lineitem")],
      ["hive-rs", hive_rs_cmd(query='select sum(l_partkey) from rs.lineitem_hive_serde',
          tbl_name='tpch6gb.lineitem', fetch_size=50000)
      ],
    ]
  ],

  [
    "Query1_Parquet_6GB", "local",
    [
      ["impala", impala_shell_cmd("select sum(l_partkey) from tpch6gb_parquet.lineitem")],
      ["impala-single-thread", impala_single_thread_cmd(
          "select sum(l_partkey) from tpch6gb_parquet.lineitem")],
      ["impala-rs", impala_on_rs_cmd(
          "select sum(l_partkey) from tpch6gb_parquet.lineitem")],
      ["native-client", native_client_cmd(
          "select l_partkey from tpch6gb_parquet.lineitem")],
      ["java-client", java_client_cmd("select l_partkey from tpch6gb_parquet.lineitem")],
      ["spark-rs", spark_q1("select l_partkey from tpch6gb_parquet.lineitem")],
    ]
  ],

  [
    "Query1_Avro_6GB", "local",
    [
      ["impala", impala_shell_cmd(
          "select sum(l_partkey) from tpch6gb_avro_snap.lineitem")],
      ["impala-single-thread", impala_single_thread_cmd(
          "select sum(l_partkey) from tpch6gb_avro_snap.lineitem")],
      ["impala-rs", impala_on_rs_cmd(
          "select sum(l_partkey) from tpch6gb_avro_snap.lineitem")],
      ["native-client", native_client_cmd(
          "select l_partkey from tpch6gb_avro_snap.lineitem")],
      ["java-client", java_client_cmd(
          "select l_partkey from tpch6gb_avro_snap.lineitem")],
      ["spark-rs", spark_q1("select l_partkey from tpch6gb_avro_snap.lineitem")],
      ["hive-rs", hive_rs_cmd(query='select sum(l_partkey) from rs.lineitem_hive_serde',
          tbl_name='tpch6gb_avro_snap.lineitem', fetch_size=50000)
      ],
    ]
  ],

  [
    "Query2_Parquet_6GB", "local",
    [
      ["impala", impala_shell_cmd(
          "select sum(l_partkey) from tpch6gb_parquet.lineitem group by l_returnflag")],
      ["impala-single-thread", impala_single_thread_cmd(
          "select sum(l_partkey) from tpch6gb_parquet.lineitem group by l_returnflag")],
      ["impala-rs", impala_on_rs_cmd(
          "select sum(l_partkey) from tpch6gb_parquet.lineitem group by l_returnflag")],
      ["native-client", native_client_cmd(
          "select l_partkey, l_returnflag from tpch6gb_parquet.lineitem")],
      ["java-client", java_client_cmd(
          "select l_partkey, l_returnflag from tpch6gb_parquet.lineitem")],
      ["spark-rs", spark_q2(
          "select l_partkey, l_returnflag from tpch6gb_parquet.lineitem")],
    ]
  ],

  [
    "Query2_Avro_6GB", "local",
    [
      ["impala", impala_shell_cmd(
          "select sum(l_partkey) from tpch6gb_avro_snap.lineitem group by l_returnflag")],
      ["impala-single-thread", impala_single_thread_cmd(
          "select sum(l_partkey) from tpch6gb_avro_snap.lineitem group by l_returnflag")],
      ["impala-rs", impala_on_rs_cmd(
          "select sum(l_partkey) from tpch6gb_avro_snap.lineitem group by l_returnflag")],
      ["native-client", native_client_cmd(
          "select l_partkey, l_returnflag from tpch6gb_avro_snap.lineitem")],
      ["java-client", java_client_cmd(
          "select l_partkey, l_returnflag from tpch6gb_avro_snap.lineitem")],
      ["spark-rs", spark_q2(
          "select l_partkey, l_returnflag from tpch6gb_avro_snap.lineitem")],
    ]
  ],

  [
    "Query1_Parquet_500GB", "cluster",
    [
      ["impala", impala_shell_cmd(
          "select count(ss_item_sk) from tpcds500gb_parquet.store_sales")],
      ["impala-rs", impala_on_rs_cmd(
          "set num_scanner_threads=32;" +
          "select count(ss_item_sk) from tpcds500gb_parquet.store_sales")],
      ["spark", spark_record_count("select ss_item_sk from tpcds500gb_parquet.store_sales")],
      ["mr", mr_record_count("select ss_item_sk from tpcds500gb_parquet.store_sales",
                             "/tmp/jenkins/recordcount_output")],
    ]
  ],

  [
    "TPCDS_Q7_Parquet_500GB", "cluster",
    [
      ["spark", spark_tpcds("q7")],
      ["spark-rs", spark_tpcds("q7", True)],
      ["impala", impala_tpcds("q7", False)],
      ["impala-rs", impala_tpcds("q7", True)],
    ]
  ],

  [
    "TPCDS_Q73_Parquet_500GB", "cluster",
    [
      ["spark", spark_tpcds("q73")],
      ["spark-rs", spark_tpcds("q73", True)],
      ["impala", impala_tpcds("q73", False)],
      ["impala-rs", impala_tpcds("q73", True)],
    ]
  ],

  [
    "TPCDS_Q88_Parquet_500GB", "cluster",
    [
      ["spark", spark_tpcds("q88")],
      ["spark-rs", spark_tpcds("q88", True)],
      ["impala", impala_tpcds("q88", False)],
      ["impala-rs", impala_tpcds("q88", True)],
    ]
  ],

  [
    "Query_1M_blocks_10K_partitions_small_files", "cluster",
    [
      ["from-cache", impala_on_rs_cmd(
          "explain select avg(id) from scale_db.num_partitions_10000_blocks_per_partition_100")],
      # ["invalidated", impala_on_rs_cmd(
      #     "invalidate metadata scale_db.num_partitions_10000_blocks_per_partition_100;" +
      #     "explain select avg(id) from scale_db.num_partitions_10000_blocks_per_partition_100")],
    ]
  ],

  [
    "Query_1M_blocks_10K_partitions_single_file", "cluster",
    [
      ["from-cache", impala_on_rs_cmd(
          "explain select avg(id) from scale_db.num_partitions_10000_blocks_per_partition_100_singlefile")],
      # ["invalidated", impala_on_rs_cmd(
      #     "invalidate metadata scale_db.num_partitions_10000_blocks_per_partition_100;" +
      #     "explain select avg(id) from scale_db.num_partitions_10000_blocks_per_partition_100_singlefile")],
    ]
  ],
  # The following tests will be run in parallel and in cluster mode.
  [
    "Query_parallel", "parallel",
    [
      ["impala", impala_shell_cmd(
          "select count(ss_item_sk) from tpcds500gb_parquet.store_sales")],
      ["impala-rs", impala_on_rs_cmd(
          "set num_scanner_threads=32;" +
          "select count(ss_item_sk) from tpcds500gb_parquet.store_sales")],
      ["spark", spark_tpcds("q7")],
      ["spark-rs", spark_tpcds("q7", True)],
      ["spark-rc-rs", spark_record_count("select ss_item_sk from tpcds500gb_parquet.store_sales")],
      ["spark-wc", spark_word_count("/test-warehouse/text_1gb", "false")],
      ["spark-wc-rs", spark_word_count("/test-warehouse/text_1gb")],
      ["mr-rc-rs", mr_record_count("select ss_item_sk from tpcds500gb_parquet.store_sales",
          "/tmp/jenkins/rc_output1")],
      ["mr-wc", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output2", "false")],
      ["mr-wc-rs", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output3")],
    ]
  ],
  [
    "Wordcount_parallel", "parallel",
    [
      ["mr-rs-1", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output1")],
      ["mr-rs_2", mr_word_count("/test-warehouse/text_1gb", "tmp/jenkins/wc_output2")],
      ["mr-1", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output3", "false")],
      ["mr-2", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output4", "false")],
      ["spark-rs-1", spark_word_count("/test-warehouse/text_1gb")],
      ["spark-rs-2", spark_word_count("/test-warehouse/text_1gb")],
      ["spark-1", spark_word_count("/test-warehouse/text_1gb", "false")],
      ["spark-2", spark_word_count("/test-warehouse/text_1gb", "false")],
    ]
  ],
  [
    "Wordcount_without_rs_parallel", "parallel",
    [
      ["mr-1", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output1", "false")],
      ["mr-2", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output2", "false")],
      ["mr-3", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output3", "false")],
      ["mr-4", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output4", "false")],
      ["spark-1", spark_word_count("/test-warehouse/text_1gb", "false")],
      ["spark-2", spark_word_count("/test-warehouse/text_1gb", "false")],
      ["spark-3", spark_word_count("/test-warehouse/text_1gb", "false")],
      ["spark-4", spark_word_count("/test-warehouse/text_1gb", "false")],
    ]
  ],
  [
    "Wordcount_with_rs_parallel", "parallel",
    [
      ["mr-rs-1", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output1")],
      ["mr-rs-2", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output2")],
      ["mr-rs-3", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output3")],
      ["mr-rs-4", mr_word_count("/test-warehouse/text_1gb", "/tmp/jenkins/wc_output4")],
      ["spark-1", spark_word_count("/test-warehouse/text_1gb")],
      ["spark-2", spark_word_count("/test-warehouse/text_1gb")],
      ["spark-3", spark_word_count("/test-warehouse/text_1gb")],
      ["spark-4", spark_word_count("/test-warehouse/text_1gb")],
    ]
  ],
]
