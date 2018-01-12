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

# Create table with 1M blocks and 10K partitions

set -u
set -e

# number of blocks per partition
BLOCKS_PER_PARTITION=100
# number of total partitions
NUM_PARTITIONS=10000
# name of database
DB_NAME=scale_db
# name of table
TBL_NAME=
# HDFS path to store all files pointed by table
HDFS_PATH=
LOCAL_OUTPUT_DIR=


# check the input is a positive integer
isPositiveInt() {
  [[ $1 =~ ^[0-9]+$ ]] && (($1 > 0))
}

# generate a single file
generateSingleFile() {
  size=$1
  if isPositiveInt $size; then
    echo "Generating single file with size=${size}MB"
    dd if=/dev/zero of=singlefile bs=$size count=1MB
    mv singlefile $LOCAL_OUTPUT_DIR
  else
    echo "File size should be positive integer"
  fi
}

# generate ${BLOCKS_PER_PARTITION} files
function generateSmallFiles {
  echo "Generating ${BLOCKS_PER_PARTITION} files"
  count=1
  while [ "${count}" -le ${BLOCKS_PER_PARTITION} ]
  do
    number=$RANDOM
    echo $number > ${LOCAL_OUTPUT_DIR}/impala_${count}.data
    # Increment count
    let "count += 1"
  done
}

# upload files to HDFS and create table in hive with many partitions
function loadTableToHive {
  # upload to HDFS
  if hadoop fs -test -d ${HDFS_PATH}; then
    echo "Dir ${HDFS_PATH} exists, just reuse it"
  else
    echo "Copying data files to HDFS"
    hadoop fs -rm -r -f ${HDFS_PATH}
    hadoop fs -mkdir -p ${HDFS_PATH}
    hadoop fs -put ${LOCAL_OUTPUT_DIR}/* ${HDFS_PATH}
  fi

  # create table in hive
  echo "create database if not exists scale_db"
  hive -e "create database if not exists scale_db"
  echo "drop table if exists ${DB_NAME}.${TBL_NAME}"
  hive -e "drop table if exists ${DB_NAME}.${TBL_NAME}"
  echo "create external table ${DB_NAME}.${TBL_NAME} (id int) partitioned by (parCol int)"
  hive -e "create external table ${DB_NAME}.${TBL_NAME} (id int) partitioned by (parCol int)"

  echo "Generating DDL statements"
  echo "use ${DB_NAME};" > ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q

  # Generate the H-SQL bulk partition DDL statement
  echo "ALTER TABLE ${TBL_NAME} ADD " >> ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q
  for p in $(seq ${NUM_PARTITIONS})
  do
    echo " PARTITION (parCol=$p) LOCATION '${HDFS_PATH}'" >> \
        ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q
  done
  echo ";" >> ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q

  # add partitions in hive table
  echo "Executing DDL via Hive"
  hive -f ${LOCAL_OUTPUT_DIR}/hive_create_partitions.q
  echo "Done! Final result in table: ${DB_NAME}.${TBL_NAME}"
}

# verify and print all variables
function verifyVar {
  if [ -z "$BLOCKS_PER_PARTITION" ]; then
    echo "BLOCKS_PER_PARTITION cannot be empty"
    exit 1
  fi

  if [ -z "$NUM_PARTITIONS" ]; then
    echo "NUM_PARTITIONS cannot be empty"
    exit 1
  fi

  if [ -z "$HDFS_PATH" ]; then
    echo "HDFS_PATH cannot be empty"
    exit 1
  fi

  if [ -z "$DB_NAME" ]; then
    echo "DB_NAME cannot be empty"
    exit 1
  fi

  if [ -z "$TBL_NAME" ]; then
    echo "TBL_NAME cannot be empty"
    exit 1
  fi

  if [ -z "$LOCAL_OUTPUT_DIR" ]; then
    echo "LOCAL_OUTPUT_DIR cannot be empty"
    exit 1
  fi

  echo "BLOCKS_PER_PARTITION=${BLOCKS_PER_PARTITION}"
  echo "NUM_PARTITIONS=${NUM_PARTITIONS}"
  echo "HDFS_PATH=${HDFS_PATH}"
  echo "DB_NAME=${DB_NAME}"
  echo "TBL_NAME=${TBL_NAME}"
  echo "LOCAL_OUTPUT_DIR=${LOCAL_OUTPUT_DIR}"
}

# remove tmp directory
function cleanup {
  if [ -n "$LOCAL_OUTPUT_DIR" ]; then
    echo "clean up ${LOCAL_OUTPUT_DIR}"
    rm -r $LOCAL_OUTPUT_DIR
  fi
}

# invalidate metadata
function invalidate {
  echo "invalidate metadata"
  /usr/bin/impala-shell -q "invalidate metadata"
}


# create hive tables with 1M blocks, 10K partitions and a single file
HDFS_PATH=/user/impala/many_blocks_num_blocks_per_partition_${BLOCKS_PER_PARTITION}_singlefile
TBL_NAME=num_partitions_${NUM_PARTITIONS}_blocks_per_partition_${BLOCKS_PER_PARTITION}_singlefile
LOCAL_OUTPUT_DIR=$(mktemp -dt "impala_test_tmp.XXXXXX")

verifyVar
generateSingleFile 13300
loadTableToHive
cleanup

# create hive tables with 1M blocks, 10K partitions and 100 small files
HDFS_PATH=/user/impala/many_blocks_num_blocks_per_partition_${BLOCKS_PER_PARTITION}
TBL_NAME=num_partitions_${NUM_PARTITIONS}_blocks_per_partition_${BLOCKS_PER_PARTITION}
LOCAL_OUTPUT_DIR=$(mktemp -dt "impala_test_tmp.XXXXXX")

verifyVar
generateSmallFiles
loadTableToHive
cleanup
