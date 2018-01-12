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
set -e

# Start up Impala
cd $IMPALA_HOME
. ${IMPALA_HOME}/bin/set-pythonpath.sh

bin/start-impala-cluster.py -s 1 --build_type=release\
    --catalogd_args="-load_catalog_in_background=false"

# Copy the JARs Hive needs to the local and HDFS AUX_JARS_PATH.
# TODO: Why does Hive need them in both places?
find $RECORD_SERVICE_HOME/java -name "*.jar" -exec cp '{}' ${HIVE_AUX_JARS_PATH} \;
find $RECORD_SERVICE_HOME/java -name "*.jar" \
    -exec  hadoop fs -put -f '{}' ${HIVE_AUX_JARS_PATH} \;

# Load the test data we need.
cd $RECORD_SERVICE_HOME
impala-shell.sh -f tests/create-benchmark-tbls.sql

# Move any existing data files to where they need to go in HDFS
hadoop fs -put -f $IMPALA_HOME/testdata/impala-data/tpch6gb/lineitem \
    /test-warehouse/tpch6gb.lineitem/

hadoop fs -put -f $IMPALA_HOME/testdata/impala-data/tpch6gb.lineitem_avro_snap/* \
    /test-warehouse/tpch6gb_avro_snap.lineitem/

# Invalidate metadata after all data is moved.
impala-shell.sh -q "invalidate metadata"

# TODO: same these files instead of regenerating them each time.
impala-shell.sh -q \
    "insert overwrite tpch6gb_parquet.lineitem select * from tpch6gb.lineitem"
