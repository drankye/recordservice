#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.

# Sets up a common environment for Jenkins builds.

# Find all java processes by this user that aren't slave.jar-related, and if
# those processes' parents are 1, kill them to death.
echo ">>> Killing left over processes"
(ps -fe -u $USER |grep java|grep -v grep |grep -v "slave.jar" |awk '{ if ($3 ~ "1") { print $2 } }'|xargs kill -9)

set -x

export IMPALA_HOME=$WORKSPACE/repos/Impala
#export IMPALA_LZO=$WORKSPACE/repos/Impala-lzo
export HADOOP_LZO=$WORKSPACE/repos/hadoop-lzo

export LLVM_HOME=/opt/toolchain/llvm-3.3
export PATH=$LLVM_HOME/bin:$PATH

if [ -n "$JENKINS_USE_TOOLCHAIN" ]; then
  export IMPALA_TOOLCHAIN=$IMPALA_HOME/toolchain/build
fi

export LD_LIBRARY_PATH=""
export LD_PRELOAD=""
# Re-source impala-config since JAVA_HOME has changed.
cd $IMPALA_HOME
. bin/impala-config.sh &> /dev/null

export PATH=/usr/lib/ccache:$PATH
export BOOST_ROOT=/opt/toolchain/boost-pic-1.55.0/
export ASAN_OPTIONS="handle_segv=0"

# We always need to clean on the ec2 machines because they are provisioned on demand
# and don't have a metastore db
${EC2_HOST_PREFIX:="impala-boost-static-burst-slave"}
${current_host:=`hostname`}
if [[ $current_host == *$EC2_HOST_PREFIX* ]]; then
  export CLEAN=true;
fi

echo ">>> Shell environment"
env
java -version
ulimit -a

echo "********************************************************************************"
echo " Environment setup for impala complete, build proper follows"
echo "********************************************************************************"
