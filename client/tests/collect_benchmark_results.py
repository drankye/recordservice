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

# This script collects benchmark results and from the mysql perf database.

from optparse import OptionParser
import MySQLdb as mdb
import sys
import os

parser = OptionParser()
parser.add_option("--perf_tbl", dest="perf_tbl", default="recordservice.perf_db",
    help="The table to insert the results into")
parser.add_option("--db_host", dest="db_host", default="vd0230.halxg.cloudera.com",
    help="The host with the mysql database")
parser.add_option("--db_user", dest="db_user", default="rs",
    help="The user name on the host")
parser.add_option("--db_password", dest="db_password", default="rs",
    help="The password for the db")
parser.add_option("--db_db", dest="db", default="recordservice",
    help="The password for the db")

options, args = parser.parse_args()

if len(sys.argv) < 4:
  sys.exit("usage: %s <workload> <days_count_to_fetch> [query]" % sys.argv[0])

con = mdb.connect(options.db_host, options.db_user, options.db_password, options.db)
with con:
  cur = con.cursor()
  workload = sys.argv[1]
  days = sys.argv[2]
  query = sys.argv[3]
  if query == "timings":
    cur.execute("select client, runtime_ms, build from perf_db " +\
      "where workload like %s AND date >= DATE_SUB(NOW(), INTERVAL %s DAY) AND "+\
      "build is not NULL " +\
      "ORDER BY client, build, date", (workload, days))
    rows = cur.fetchall()
    print 'client', '\t', 'runtime', '\t', 'build_number'
    for row in rows:
      print row[0], '\t', row[1], '\t', row[2]
  elif query == "max_build":
    cur.execute("select max(build) from perf_db " +\
      "where workload like %s AND date >= DATE_SUB(NOW(), INTERVAL %s DAY) AND "+\
      "build is not NULL", (workload, days))
    rows = cur.fetchall()
    print rows[0][0]
  else:
    sys.exit("Query must be: timings OR max_build")
