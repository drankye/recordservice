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
# Loads the test tables.

# Exit on reference to uninitialized variables and non-zero exit codes
set -u
set -ex

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

# Copy the TPC-H dataset
DATASRC="http://util-1.ent.cloudera.com/impala-test-data/"
DATADST=${IMPALA_HOME}/testdata/impala-data

if [ ! -d ${DATADST}/tpch ]; then
  mkdir -p ${DATADST}
  pushd ${DATADST}
  wget -q --no-clobber http://util-1.ent.cloudera.com/impala-test-data/tpch.tar.gz
  tar -xzf tpch.tar.gz
  popd
fi

# Load the test data we need.
cd $RECORD_SERVICE_HOME

if [ $USE_SENTRY -eq 1 ]; then
  set +e
  impala-shell.sh -q "DROP ROLE TEST_ROLE"
  set -e
  impala-shell.sh -q "CREATE ROLE TEST_ROLE"
  impala-shell.sh -q "GRANT ALL ON SERVER TO ROLE TEST_ROLE"
  impala-shell.sh -q "GRANT ROLE TEST_ROLE TO GROUP \`$USER\`"
fi

impala-shell.sh -f tests/create-tbls.sql

# Move any existing data files to where they need to go in HDFS
hadoop fs -mkdir -p /test-warehouse/tpch.nation/
hadoop fs -put -f $IMPALA_HOME/testdata/impala-data/tpch/nation/*\
    /test-warehouse/tpch.nation/
hadoop fs -mkdir -p /test-warehouse/tpch_nation_parquet/
hadoop fs -put -f $IMPALA_HOME/testdata/nation.parq /test-warehouse/tpch_nation_parquet/

# Move UDF jar to HDFS
hadoop fs -mkdir -p /test-warehouse/udfs/
hadoop fs -put -f $IMPALA_HOME/testdata/udfs/hive-mask-udf.jar /test-warehouse/udfs
hadoop fs -mkdir -p /test-warehouse/test-uri/
hadoop fs -put -f $IMPALA_HOME/testdata/udfs/hive-mask-udf.jar /test-warehouse/test-uri

# Create UDFs
hive -e "DROP FUNCTION IF EXISTS mask; \
CREATE FUNCTION mask AS 'com.cloudera.hive.udf.example.Mask' USING JAR 'hdfs:///test-warehouse/udfs/hive-mask-udf.jar'; \
DROP DATABASE IF EXISTS udf CASCADE; CREATE DATABASE udf; \
CREATE FUNCTION udf.mask1 AS 'com.cloudera.hive.udf.example.Mask' USING JAR 'hdfs:///test-warehouse/test-uri/hive-mask-udf.jar'; \
CREATE FUNCTION udf.mask2 AS 'com.cloudera.hive.udf.example.Mask' USING JAR 'hdfs:///test-warehouse/udfs/hive-mask-udf.jar';"

# Invalidate metadata after all data is moved.
impala-shell.sh -q "invalidate metadata"
