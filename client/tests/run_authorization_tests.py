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

from optparse import OptionParser
import json
import os
import subprocess
import sys
from time import sleep, time

TEST_USER = "sentry_test_user"
TEST_ROLE = "SENTRY_TEST_ROLE"
AUDIT_LOG_DIR = "/tmp/recordservice/audit-test"

parser = OptionParser()
parser.add_option("--host", dest="host", default="localhost",
    help="Host to connect to.")
parser.add_option("--audit_log_dir", dest="audit_log_dir", default=AUDIT_LOG_DIR,
    help="Directory where the audit logs should go to.")
(options, args) = parser.parse_args()
IMPALA_SHELL = os.environ['IMPALA_HOME'] + "/bin/impala-shell.sh"

# A dummy class used to store test output
class TestOutput:
  msg = ""

# Runs 'cmd' and waits for it to finish. Add stdout to 'output'
# if it is not None. Also add stderr if 'stderr' is True.
def run_shell_cmd(cmd, output = None, stderr = False):
  tmp_file = "/tmp/sentry_test_result"
  cmd += " > " + tmp_file
  if stderr: cmd += " 2>&1"
  ret = subprocess.call(cmd, shell = True)
  if output:
    with open(tmp_file, 'r') as f:
      for line in f:
        output.msg += line.strip() + '\n'

  if ret != 0:
    raise Exception("Failed to run cmd: '%s' ret=%s" % (cmd, ret))


def run_impala_query(query):
  return run_shell_cmd(IMPALA_SHELL + " -q \"" + query + "\"")

# The audit event might not show up immediately (the audit logs are flushed to disk
# on regular intervals), so poll the audit event logs until a matching record is
# found.
def wait_and_find_audit_record(timeout_secs=120):
  start_time = time()
  while time() - start_time < timeout_secs:
    files = os.listdir(options.audit_log_dir)
    # Should have exactly one file. Wait until the file is not empty.
    if len(files) > 0 and \
       os.stat(os.path.join(options.audit_log_dir, files[0])).st_size > 0:
      return files[0]
    sleep(1)
  return None

# Construct a list of expected results (each of which is a dictionary).
def make_expected_records():
  result = list()
  m1 = dict()
  m1['authorization_failure'] = True
  m1['status'] = "AuthorizationException: user 'sentry_test_user' does " \
                 "not have full access to the path '/test-warehouse'\n"
  m1['catalog_objects'] = []
  m2 = dict()
  m2['authorization_failure'] = False
  m2['status'] = ''
  m2['catalog_objects'] = \
    [{'name':'rs_tmp_db', 'object_type':'TABLE','privilege':'SELECT'}]
  m3 = dict()
  m3['authorization_failure'] = True
  m3['status'] = "AuthorizationException: User 'sentry_test_user' does " \
                 "not have privileges to execute 'SELECT' on: tpch.nation\n"
  m3['catalog_objects'] = []
  m4 = dict()
  m4['authorization_failure'] = False
  m4['status'] = ''
  m4['catalog_objects'] = \
    [{'name':'tpch.nation','object_type':'TABLE','privilege':'SELECT'}]
  result.append(m1)
  result.append(m2)
  result.append(m3)
  result.append(m4)
  return result

def verify_audit_events():
  audit_file_name = wait_and_find_audit_record()
  ## wait for the file to be fully populated
  sleep(30)
  if not audit_file_name:
    return (False, "Couldn't find an audit log record under {0}"
            .format(options.audit_log_dir))

  expected_records = make_expected_records()
  error_msg = "All the records that are matched:\n"
  with open(os.path.join(options.audit_log_dir, audit_file_name)) as audit_log_file:
    for line in audit_log_file.readlines():
      try:
        json_dict = json.loads(line)
      except ValueError:
        error_msg += "Got ValueError for input line: " + line
        continue
      json_dict = json_dict[min(json_dict)] # Remove the wrapper

      # Check user and impersonator
      if json_dict['user'] != TEST_USER:
        return (False, "Expected 'user' to be '{0}', but found '{1}' in '{2}'"
                .format(TEST_USER, json_dict['user'], json_dict))
      elif json_dict['impersonator'] is not None:
        return (False, "Expected 'impersonator' to be None, but found '{0}' in '{1}'"
                .format(json_dict['impersonator'], json_dict))

      # Remove the random part from the tmp table name
      if len(json_dict['catalog_objects']) == 1 and \
         json_dict['catalog_objects'][0]['name'].find('rs_tmp_db') >= 0:
        json_dict['catalog_objects'][0]['name'] = 'rs_tmp_db'

      # Filter out uninteresting entries
      for key in ['query_id','session_id','start_time','user','impersonator',
                  'network_address', 'sql_statement', 'statement_type']:
        try:
          json_dict.pop(key)
        except KeyError:
          return (False, "Couldn't find required key '{0}' in '{1}'"
                  .format(key, json_dict))

      error_msg += str(json_dict) + "\n"
      if json_dict in expected_records:
        error_msg += "matched\n"
        expected_records.remove(json_dict)
    if not expected_records:
      return (True, "")
    else:
      error_msg += "Not all expected results are matched. Remaining:\n"
      error_msg += '\n'.join([str(r) for r in expected_records])
      return (False, error_msg)

if __name__ == "__main__":
  print("Running against host: " + options.host +
        ", and audit log dir: " + options.audit_log_dir)

  try:
    run_shell_cmd("sudo /usr/sbin/useradd " + TEST_USER)
  except Exception:
    pass

  run_impala_query(
    "DROP VIEW IF EXISTS tpch.nation_view;" +
    "CREATE VIEW tpch.nation_view AS SELECT n_name FROM tpch.nation")

  try:
    run_impala_query("CREATE ROLE " + TEST_ROLE)
  except Exception as e:
    pass

  run_impala_query(
    "GRANT ROLE " + TEST_ROLE + " TO GROUP " + TEST_USER + ";" +
    "GRANT SELECT ON TABLE tpch.nation_view TO ROLE " + TEST_ROLE + ";" +
    "GRANT ALL ON URI 'hdfs:/test-warehouse/tpch.nation' TO ROLE " + TEST_ROLE + ";" +
    "GRANT SELECT(n_name, n_nationkey) ON TABLE tpch.nation to ROLE " + TEST_ROLE + ";" +
    "GRANT ALL ON URI 'hdfs:/test-warehouse/udfs' TO ROLE " + TEST_ROLE + ";" +
    "GRANT SELECT ON DATABASE udf to ROLE " + TEST_ROLE)

  # Need to wait for a while before the Sentry change is populated.
  # TODO: how to avoid this waiting?
  time_to_wait = 60
  print "Waiting for " + str(time_to_wait) + " seconds..."
  sleep(time_to_wait)

  try:
    test_output = TestOutput()
    os.chdir(os.environ['RECORD_SERVICE_HOME'] + '/java/core')
    run_shell_cmd(
      "mvn test -Duser.name=" + TEST_USER +
      " -Dtest=TestSentry", test_output, True)
    os.chdir(os.environ['RECORD_SERVICE_HOME'] + '/java/hcatalog-pig-adapter')
    run_shell_cmd(
      "mvn test -Duser.name=" + TEST_USER +
      " -Dtest=TestHCatRSLoaderSentry", test_output, True)
  except Exception as e:
    print e.message
  finally:
    print test_output.msg
    run_shell_cmd("sudo /usr/sbin/userdel " + TEST_USER)
    run_impala_query("DROP ROLE " + TEST_ROLE)

    # Needs to wait a few seconds for the audit events
    (success, error_msg) = verify_audit_events()
    if success:
      print "Successfully verified audit events."
    else:
      sys.exit("Failed at verifying audit events:\n" + error_msg)


