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
# Loads the test tables for S3.
# TODO: reconcile this with load-test-data.

# Exit on reference to uninitialized variables and non-zero exit codes
set -u
set -e

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

impala-shell.sh -f tests/create-tbls-s3.sql

