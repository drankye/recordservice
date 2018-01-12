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

# Basic tests to verify kerberos connections work. Everything about this is bad.
#  - Assumes a specific server set up by system with some tables.
#  - Hack of a "test framework"
# The machine this is running on is assumed to have kerberos connectivity to the
# host machine.

from optparse import OptionParser
import os
import subprocess

parser = OptionParser()
parser.add_option("--host", dest="host", default="vd0224.halxg.cloudera.com",
    help="Host to connect to.")
(options, args) = parser.parse_args()
IMPALA_SHELL = os.environ['IMPALA_HOME'] + "/bin/impala-shell.sh"

# Runs 'cmd' and waits for it to finish. Returns stdout.
def run_shell_cmd(cmd):
  tmp_file = "/tmp/kerb_test_results"
  cmd += "> " + tmp_file
  ret = subprocess.call(cmd, shell = True)
  if ret != 0:
    raise Exception("Failed to run cmd: %s ret=%s" % (cmd, ret))

  result = ""
  with open(tmp_file, 'r') as f:
    for line in f:
      result += line.strip()
  return result

def verify_impala_query(sql, expected):
  cmd = IMPALA_SHELL + " -k -B -i " + options.host + " -q \"" + sql + "\""
  actual = run_shell_cmd(cmd)
  print actual
  if actual != expected:
    raise Exception("Incorrect results. Expected: %s Actual: %s" % (expected, actual))

if __name__ == "__main__":
  print("Running against host: " + options.host)
  verify_impala_query("select count(*) from sample_07", "823")
  verify_impala_query("select sum(salary) from sample_07", "39282210")

