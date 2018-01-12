#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.

# Sets up a common environment for Jenkins builds.
# We split this up from preamble_impala.sh because Impala uses a custom version of
# thrift that it builds.

echo "********************************************************************************"
echo " Building ${JOB_NAME} #${BUILD_NUMBER} "
echo " Node: ${NODE_NAME} / `hostname`"
echo " Branch: ${GIT_BRANCH}@${GIT_COMMIT}"
echo " ${BUILD_URL}"
echo " Path: ${WORKSPACE}"
echo "********************************************************************************"

# Find all java processes by this user that aren't slave.jar-related, and if
# those processes' parents are 1, kill them to death.
echo ">>> Killing left over processes"
(ps -fe -u $USER |grep java|grep -v grep |grep -v "slave.jar" |awk '{ if ($3 ~ "1") { print $2 } }'|xargs kill -9)

echo ">>> Setting up Jenkins environment..."
echo ">>> Mounting toolchain"
. /mnt/toolchain/toolchain.sh
export RECORD_SERVICE_HOME=$WORKSPACE/repos/RecordServiceClient

export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

export JAVA_HOME=$JAVA7_HOME
export JAVA64_HOME=$JAVA7_HOME
export PATH=$JAVA_HOME/bin:$PATH
export PATH=/usr/lib/ccache:$PATH
export THRIFT_HOME=/opt/toolchain/thrift-0.9.0

# Enable core dumps
ulimit -c unlimited

echo ">>> Shell environment"
env
java -version
ulimit -a

echo "********************************************************************************"
echo " Environment setup for RecordService complete, build proper follows"
echo "********************************************************************************"

