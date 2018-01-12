Copyright (c) 2015, Cloudera, inc.

## Overview
RecordService is new service provides a common API for frameworks such as MR and Spark to read data from Hadoop storage managers and return them as canonical records. This eliminates the need for applications and Hadoop components to support individual file formats, handle data security, perform auditing, implement sophisticated IO scheduling and other common processing that is at the bottom of any computation. 

This repo contains the RecordService service definition, the client libraries to use the RecordService, tests and some automation scripts.

## Getting Started
### Prereqs
We require a thrift 0.9+ compiler, maven, cmake and java 7.

Increase your maven heap size:

    export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

On Ubuntu:

    TODO

On OSX:

    brew install thrift maven cmake

### Setup
After cloning the repo for the first time you will need to run:

    cd <Project root directory>
    source ./config.sh
    thirdparty/download_thirdparty.sh
    thirdparty/build_thirdparty.sh

### Building the client repo

    cd <Project root directory>
    source ./config.sh
    cmake .
    make
    cd $RECORD_SERVICE_HOME/java
    mvn package -DskipTests

This will build all the client artifacts and tests. Note that on OSX, the c++ client libraries are currently not supported and not built.

### Running the tests
The tests require a running RecordService server running with the test data loaded. If this server already exists, you can direct the tests to that server by setting RECORD_SERVICE_PLANNER_HOST in your environment. This defaults to localhost if not set.

    export RECORD_SERVICE_PLANNER_HOST=<>
    cd $RECORD_SERVICE_HOME/java
    mvn test

This will run all of the java client tests.

### Setting up eclipse
The client is a mvn project and can be simply imported from eclipse. If running against a remote server, be sure to set RECORD_SERVICE_PLANNER_HOST before starting up eclipse. 

On OSX this can be done by opening a terminal and doing

     export RECORD_SERVICE_PLANNER_HOST=<>
     open -a Eclipse

### First steps
The repo comes with a few samples that demonstrate how to use the RecordService client APIs, as well as examples of how to integrate with MapReduce and Spark. The examples can be found in
* java/examples
* java/examples-spark

### Repo structure
* api/: Thrift file(s) containing the RecordService API definition
* cpp/: cpp sample and client code
* java/: java sample and client code
* tests/: Scripts to load test data, run tests and run benchmarks.
* jenkins/: Scripts intended to be run from jenkins builds.
