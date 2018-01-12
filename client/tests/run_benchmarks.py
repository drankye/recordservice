#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

# This script runs the benchmarks, outputting the results to a db.

from optparse import OptionParser
import os
import sys
import subprocess
import time
from datetime import datetime
import threading

import benchmarks

parser = OptionParser()
parser.add_option("--iterations", dest="iterations", default=1, type="int",
    help="Number of iterations to run for each case")
parser.add_option("--warmup_iterations", dest="warmup_iterations", default=0, type="int",
    help="Number of warmup iterations to run for each case. These iterations are not\
      counted in the timing.")
parser.add_option("--suite_warmup_iterations", dest="suite_warmup_iterations",
    default=0, type="int",
    help="Number of warmup iterations to run for each suite. These iterations are not\
      counted in the timing. This is useful to get data in the buffer cache.")

parser.add_option("--log_file", dest="log_file", default="/tmp/rs_benchmark.log",
    help="File that contains stdout/stderr from running each case")
parser.add_option("--result_sql_file", dest="result_sql_file",
    default="/tmp/rs_benchmark.sql",
    help="File that contains the benchmark results as sql statement.")

parser.add_option("--build_number", dest="build_number", default=-1, type="int",
    help="The (jenkins) build number. -1 if not from jenkins.")

parser.add_option("--cluster", dest="cluster", default=False, action="store_true",
    help="If true, this is running the cluster benchmarks")
parser.add_option("--impalad", dest="impalad", default="localhost",
    help="The impalad to connect to.")

# This option is used to run and generate results without storing them in the
# perf db. This is useful for unofficial runs. If this is set, build_number and
# db_host are ignored.
parser.add_option("--result_tsv_dir", dest = "result_tsv_dir", default="",
    help="Directory to put resulting tsv files.")

parser.add_option("--perf_tbl", dest="perf_tbl", default="recordservice.perf_db",
    help="The table to insert the results into")
parser.add_option("--db_host", dest="db_host", default="",
    help="The host with the mysql database. If NULL, does not write to db")
parser.add_option("--db_user", dest="db_user", default="rs",
    help="The user name on the host")
parser.add_option("--db_password", dest="db_password", default="rs",
    help="The password for the db")

threadLock = threading.Lock()
# Runs 'cmd' and waits for the return value.
def run_shell_cmd(cmd):
  cmd += " >> " + options.log_file + " 2>&1"
  ret = subprocess.call(cmd, shell = True)
  if ret != 0:
    raise Exception("Failed to run cmd: %s ret=%s" % (cmd, ret))
  return 0

# Schema is:
# mysql> describe perf_db;
# +----------+----------------+------+-----+---------+-------+
# | Field    | Type           | Null | Key | Default | Extra |
# +----------+----------------+------+-----+---------+-------+
# | date     | datetime       | YES  |     | NULL    |       |
# | build    | int(11)        | YES  |     | NULL    |       |
# | version  | varchar(100)   | YES  |     | NULL    |       |
# | workload | varchar(100)   | YES  |     | NULL    |       |
# | client   | varchar(100)   | YES  |     | NULL    |       |
# | time_ms  | float          | YES  |     | NULL    |       |
# | labels   | varchar(2000)  | YES  |     | NULL    |       |
# +----------+----------------+------+-----+---------+-------+
def to_sql(suite, case, timing_ms):
  cmd = "insert into " + options.perf_tbl + " values("
  cmd += "\"" + str(datetime.now()) + "\", "
  if options.build_number >= 0:
    cmd += str(options.build_number) + ", "
  else:
    cmd += "NULL, "
  # TODO: get version
  cmd += "NULL, "
  cmd += "\"" + suite[0] + "\", "
  cmd += "\"" + case[0] + "\", "
  cmd += str(timing_ms) + ", "
  # TODO: plumb labels
  cmd += "NULL"
  cmd += ");"
  return cmd

# Replaces LOCALHOST with the cluster host.
# TODO: this works fine for impala-shell but we might need something more complex
# for the other things.
def replace_host(cmd):
  if (options.cluster):
    return cmd.replace("LOCALHOST", options.impalad)
  return cmd

def run_case(case, results_sql, result_tsv, suite):
  print "  Running case: " + case[0]
  sys.stdout.flush()
  cmd = replace_host(case[1])

  for x in range(0, options.warmup_iterations):
    run_shell_cmd(cmd)

  iterations = options.iterations;
  if (suite[0] == "TPCDS_Q88_Parquet_500GB"):
    iterations = 1

  for x in range(0, iterations):
    start = time.time() * 1000
    run_shell_cmd(cmd)
    timing_ms = time.time() * 1000 - start
    if (suite[1] == "parallel"):
      # Get lock to synchronize threads
      threadLock.acquire()
    results_sql.write(to_sql(suite, case, timing_ms) + "\n")
    if (result_tsv != None):
      # Write the results in the form that's normally output from the DB.
      # Write a marker build number.
      result_tsv.write(case[0] + "\t" + str(timing_ms) + "\tBUILD_NUMBER\n")
    if (suite[1] == "parallel"):
      # Free lock to release next thread
      threadLock.release()
      # parallel test only run once
      break

def run_suite(suite, results_sql):
  if (options.cluster):
    if (suite[1] != "cluster" and suite[1] != "parallel"):
      print "Skipping local suite: " + suite[0]
      return
  else:
    if (suite[1] != "local"):
      print "Skipping cluster / parallel suite: " + suite[0]
      return
  print "Running benchmark suite: " + suite[0]

  cases = suite[2]

  if len(cases) == 0:
    print "Cannot run suite with no cases. suite=" + suite[0]
    sys.exit(1)

  result_tsv = None
  if (options.result_tsv_dir != ""):
    result_tsv = open(options.result_tsv_dir + "/" + suite[0], "w")

  if options.suite_warmup_iterations > 0:
    # Just run the first case for these many iterations
    cmd = replace_host(cases[0][1])
    for x in range(0, options.suite_warmup_iterations):
      run_shell_cmd(cmd)

  if (suite[1] == "parallel"):
    # Running all cases in parallel
    print " Running all cases in parallel: ", len(cases)
    threads = []
    for case in cases:
      t = threading.Thread(target=run_case, args=(case, results_sql, result_tsv, suite))
      t.start()
      threads.append(t)
    for t in threads:
      t.join()
    print "Finish the parallel test " + suite[0]
  else:
    for case in cases:
      run_case(case, results_sql, result_tsv, suite)

if __name__ == "__main__":
  benchmark_start_time_ms = time.time() * 1000

  (options, args) = parser.parse_args()
  run_shell_cmd("rm -f " + options.log_file)

  results_sql = open(options.result_sql_file, "w")
  if (options.result_tsv_dir != ""):
    run_shell_cmd("mkdir -p " + options.result_tsv_dir)

  for suite in benchmarks.benchmarks:
    run_suite(suite, results_sql)

  results_sql.close()

  duration_ms = time.time() * 1000 - benchmark_start_time_ms
  print("Finished benchmark: " + str(duration_ms) + "ms")

  if (options.db_host != "" and options.result_tsv_dir == ""):
    print("Inserting results into db@" + options.db_host + "\n")
    cmd = "mysql -h " + options.db_host + " -u " + options.db_user +\
        " -p" + options.db_password + " < " + options.result_sql_file
    run_shell_cmd(cmd)

