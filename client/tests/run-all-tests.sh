#!/usr/bin/env bash
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
#

# Runs all the tests.

# Exit on reference to uninitialized variables and non-zero exit codes
set -u
set -e

export RECORD_SERVICE_QUICKSTART_VM="${RECORD_SERVICE_QUICKSTART_VM:-"False"}"

cd $IMPALA_HOME
source bin/set-classpath.sh

function not_quickstart_vm() {
  if [ $RECORD_SERVICE_QUICKSTART_VM != "True" ] && \
       [ $RECORD_SERVICE_QUICKSTART_VM != "true" ]; then
    return 0
  else
    return 1
  fi
}

if not_quickstart_vm ; then
  bin/start-impala-cluster.py --kill
fi

cd $RECORD_SERVICE_HOME

if not_quickstart_vm ; then
  echo "Running mini cluster tests."
  export RUN_MINI_CLUSTER_TESTS=true
  make test

  echo "Running non-mini cluster tests."

  # Start up a recordserviced and run the client tests. Note that at this point
  # there is no impala cluster running
  cd $IMPALA_HOME
  bin/start-impala-cluster.py -s 1 --catalogd_args="-load_catalog_in_background=false" \
    --rs_args="-rs_disable_udf=false"
  # TODO: update bin/start-impala-cluster.py
  killall -9 impalad
  killall -9 statestored
  killall -9 catalogd

  cd $RECORD_SERVICE_HOME
  unset RUN_MINI_CLUSTER_TESTS
  make test
fi

mvn clean install -f $RECORD_SERVICE_HOME/java/pom.xml

AUDIT_LOG_DIR=/tmp/recordservice/audit-test

if not_quickstart_vm ; then
  if [ -d ${AUDIT_LOG_DIR} ]; then
    # Clean the audit log dir before running related tests
    rm ${AUDIT_LOG_DIR}/*
  else
    mkdir -p ${AUDIT_LOG_DIR}
  fi

  # Start up the cluster for the tests that need an Impala cluster already running.
  cd $IMPALA_HOME
  bin/start-impala-cluster.py -s 1 --catalogd_args="-load_catalog_in_background=false" \
    --rs_args="-audit_event_log_dir=${AUDIT_LOG_DIR} -rs_disable_udf=false"

  # Run Authorization tests
  export IGNORE_SENTRY_TESTS=false
  cd $RECORD_SERVICE_HOME/tests
  ./run_authorization_tests.py --audit_log_dir=${AUDIT_LOG_DIR}

  # Run Hive SerDe test
  cd $IMPALA_HOME/fe
  mvn -Dtest=HiveSerDeExecutorTest test


  cd $IMPALA_HOME/tests
  ./run-tests.py query_test/test_hive_serde.py
  ./run-tests.py query_test/test_recordservice.py
fi
