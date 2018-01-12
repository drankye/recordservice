#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.

# Loads data and starts the cluster to run local benchmarks.

USE_SENTRY=1

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -no_sentry)
      USE_SENTRY=0
      ;;
    -help|*)
      echo "[-no_sentry] : If used, no sentry roles are created"
      exit 1
      ;;
  esac
done

# Builds record service client.
export TARGET_BUILD_TYPE=Release
source $WORKSPACE/repos/RecordServiceClient/jenkins/preamble_rs.sh
cd $RECORD_SERVICE_HOME
make clean
# Build
. $RECORD_SERVICE_HOME/jenkins/build_rs.sh

# Builds record service server
source $WORKSPACE/repos/RecordServiceClient/jenkins/preamble_impala.sh
# Build
. $RECORD_SERVICE_HOME/jenkins/build_impala.sh

echo ">>> Starting all services "
cd $IMPALA_HOME
. testdata/bin/run-all.sh || { echo "run-all.sh failed"; exit 1; }

cd $IMPALA_HOME
. ${IMPALA_HOME}/bin/set-pythonpath.sh

set -e

# Move the jars to where we need them
find $RECORD_SERVICE_HOME/java -name "*recordservice-hive*.jar" -exec cp '{}' ${HIVE_AUX_JARS_PATH} \;
find $RECORD_SERVICE_HOME/java -name "*recordservice-core*.jar" -exec cp '{}' ${HIVE_AUX_JARS_PATH} \;
hadoop fs -mkdir -p ${HIVE_AUX_JARS_PATH}
hadoop fs -put -f ${HIVE_AUX_JARS_PATH}/*.jar ${HIVE_AUX_JARS_PATH}

if $LOAD_DATA; then
  # Load test data
  echo ">>> Starting impala and loading benchmark data"

  # Copy the TPC-H dataset
  DATASRC="http://util-1.ent.cloudera.com/impala-test-data/"
  DATADST=${IMPALA_HOME}/testdata/impala-data

  mkdir -p ${DATADST}
  pushd ${DATADST}
  wget -q --no-clobber http://util-1.ent.cloudera.com/impala-test-data/tpch6gb.tar.gz
  wget -q --no-clobber http://util-1.ent.cloudera.com/impala-test-data/tpch6gb_avro_snap.tar.gz
  tar -xzf tpch6gb.tar.gz
  tar -xf tpch6gb_avro_snap.tar.gz
  popd

  cd $RECORD_SERVICE_HOME/tests
  ./load-benchmark-data.sh
else
  echo ">>> Starting impala"
  cd $IMPALA_HOME
  bin/start-impala-cluster.py -s 1 --build_type=release --catalogd_args="-load_catalog_in_background=false"
fi

if [ $USE_SENTRY -eq 1 ]; then
  set +e
  impala-shell.sh -q "DROP ROLE TEST_ROLE"
  set -e
  impala-shell.sh -q "CREATE ROLE TEST_ROLE"
  impala-shell.sh -q "GRANT ALL ON SERVER TO ROLE TEST_ROLE"
  impala-shell.sh -q "GRANT ROLE TEST_ROLE TO GROUP \`$USER\`"
fi

cd $RECORD_SERVICE_HOME/java
mvn install -DskipTests

echo ">>> Impala version"
cd $IMPALA_HOME
bin/impala-shell.sh -q "select version()"

