#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.

# Builds impala, with output redirected to $WORKSPACE/buildall.log
# TARGET_BUILD_TYPE can be set to Release/Debug

echo "********************************************************************************"
echo " building RecordService daemons."
echo "********************************************************************************"

BUILD_ARGS="-skiptests"

# Get the HDFS version. If this changes, we need to format.
HDFS_VERSION=`/bin/grep layoutVersion $CLUSTER_DIR/cdh5/node-1/data/dfs/nn/current/VERSION | cut -d= -f2`

if [[ $CLEAN ]] && $CLEAN
then
  echo ">>> Cleaning workspace"
  pushd $IMPALA_HOME
  git clean -dfx && git reset --hard HEAD
  BUILD_ARGS="$BUILD_ARGS -format_metastore"
  popd
fi

if [ -n "$JENKINS_USE_TOOLCHAIN" ]; then
  echo ">>> Bootstrapping toolchain."
  cd $RECORD_SERVICE_HOME
  impala-python jenkins/bootstrap_toolchain.py || { echo "toolchain bootstrap failed"; exit 1; }
fi

# Version for CDH5.4 is -60
if [ $HDFS_VERSION -ne -60 ]
then
  # This is a somewhat general hack for additional steps that need to be taken when
  # upgrading CDH
  echo ">>> Formatting HDFS"
  BUILD_ARGS="$BUILD_ARGS -format"
fi

pushd $IMPALA_HOME
rm -f ./bin/version.info
rm -f ./CMakeCache.txt
pushd $IMPALA_HOME/fe
mvn clean
popd

echo "Build Args: $BUILD_ARGS"

if [ ! -d "$IMPALA_CYRUS_SASL_INSTALL_DIR" ]; then
  echo "Building sasl"
  bin/build_thirdparty.sh -sasl
fi

./buildall.sh $BUILD_ARGS > $WORKSPACE/buildall.log 2>&1 ||\
    { tail -n 100 $WORKSPACE/buildall.log; echo "buildall.sh failed"; exit 1; }
popd

