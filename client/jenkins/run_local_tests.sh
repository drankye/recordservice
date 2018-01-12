#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.

# Loads data and starts the cluster to run local tests

export S3=${S3:-0}
# Builds record service client.
source $WORKSPACE/repos/RecordServiceClient/jenkins/preamble_rs.sh
cd $RECORD_SERVICE_HOME
make clean
# Build
. $RECORD_SERVICE_HOME/jenkins/build_rs.sh

# Builds record service server
source $WORKSPACE/repos/RecordServiceClient/jenkins/preamble_impala.sh
# Build
. $RECORD_SERVICE_HOME/jenkins/build_impala.sh

echo ">>> Starting all services"
cd $IMPALA_HOME
. testdata/bin/run-all.sh

# Start up Impala
cd $IMPALA_HOME
. ${IMPALA_HOME}/bin/set-pythonpath.sh

# Run backend unit tests
bin/run-backend-tests.sh

bin/start-impala-cluster.py -s 1 --start_recordservice --catalogd_args="-load_catalog_in_background=false" \
  --rs_args="-rs_disable_udf=false"

if [ $S3 -eq 1 ]; then
  echo ">>> Loading test data"
  . $RECORD_SERVICE_HOME/tests/load-test-data-s3.sh

  echo ">>> Running tests"
  cd $RECORD_SERVICE_HOME/java/core
  mvn -Dtest=*S3* test
else
  echo ">>> Loading test data"
  . $RECORD_SERVICE_HOME/tests/load-test-data.sh
  echo ">>> Running tests"
  . $RECORD_SERVICE_HOME/tests/run-all-tests.sh
fi
