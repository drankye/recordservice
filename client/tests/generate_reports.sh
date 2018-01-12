#!/bin/bash -xe
########################################################################
# Copyright (c) 2014 Cloudera, Inc.

# This script pulls data from the perf database and uses R to generate
# the results plot.

set -e

generate_report() {
  local WORKLOAD=$1
  local DAYS=$2
  local RESULTS_DIR=$3

  ./collect_benchmark_results.py "$WORKLOAD" $DAYS timings > /tmp/data.tsv
  if [ -n "$RESULTS_DIR" ] && [ -e "$RESULTS_DIR/$WORKLOAD" ]; then
    # In this case, we have local results (not in DB) that need to be merged into
    # /tmp/data/tsv. The local results don't have a build number and we want to
    # assign it the next highest build number.
    local MAX_BUILD=`./collect_benchmark_results.py "$WORKLOAD" $DAYS max_build`
    NEXT_BUILD=$((MAX_BUILD+1))

    # Replace the BUILD_NUMBER in results dir with the max build number + 1
    # This means the results from RESULTS_DIR always comes right after the results
    # from the DB.
    sed -i "s/BUILD_NUMBER/$NEXT_BUILD/" "$RESULTS_DIR/$WORKLOAD"

    # Append the local results (not stored in the data base) before plotting.
    cat "$RESULTS_DIR/$WORKLOAD" >> /tmp/data.tsv
  fi
  Rscript generate_runtime_plots.R /tmp/data.tsv $WORKLOAD
}

# The number of days to go back to get results.
DAYS=15

# Additional results to append to the ones stored in the DB. These results are
# not stored in the DB.
RESULTS_DIR=$1

generate_report "Query1_Text_6GB" $DAYS $RESULTS_DIR
generate_report "Query1_Parquet_6GB" $DAYS $RESULTS_DIR
generate_report "Query1_Avro_6GB" $DAYS $RESULTS_DIR
generate_report "Query2_Parquet_6GB" $DAYS $RESULTS_DIR
generate_report "Query2_Avro_6GB" $DAYS $RESULTS_DIR
generate_report "Query1_Parquet_500GB" $DAYS $RESULTS_DIR
generate_report "TPCDS_Q88_Parquet_500GB" $DAYS $RESULTS_DIR
generate_report "TPCDS_Q7_Parquet_500GB" $DAYS $RESULTS_DIR
generate_report "TPCDS_Q73_Parquet_500GB" $DAYS $RESULTS_DIR
generate_report "Query_1M_blocks_10K_partitions_small_files" $DAYS $RESULTS_DIR
generate_report "Query_1M_blocks_10K_partitions_single_file" $DAYS $RESULTS_DIR
generate_report "Query_parallel" $DAYS $RESULTS_DIR
generate_report "Wordcount_parallel" $DAYS $RESULTS_DIR
generate_report "Wordcount_without_rs_parallel" $DAYS $RESULTS_DIR
generate_report "Wordcount_with_rs_parallel" $DAYS $RESULTS_DIR

mkdir -p $RECORD_SERVICE_HOME/benchmark_results
rm -f $RECORD_SERVICE_HOME/benchmark_results/*
mv ./*.png $RECORD_SERVICE_HOME/benchmark_results
