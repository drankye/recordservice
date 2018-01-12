#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.

# Builds impala, with output redirected to $WORKSPACE/buildall.log
# TARGET_BUILD_TYPE can be set to Release/Debug

echo
echo "********************************************************************************"
echo " building RecordService client."
echo "********************************************************************************"
echo

pushd $RECORD_SERVICE_HOME
git clean -dfx && git reset --hard HEAD

./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh

cmake .
make

if [[ -z $RECORD_SERVICE_SKIP_JAVA_CLIENT ]]; then
  pushd $RECORD_SERVICE_HOME/java
  mvn install -DskipTests
  popd
fi

popd
