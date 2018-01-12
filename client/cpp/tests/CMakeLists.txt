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

# where to put generated libraries.
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/recordservice")

# where to put generated binaries.
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/recordservice")

add_library(TestUtil
  external-mini-cluster.cc
  subprocess.cc
)

SET(LINK_LIBS
  TestUtil
  RecordServiceThrift
  thriftstatic
  gtest
  gflagsstatic)

add_executable(start-external-mini-rs-cluster start-external-mini-rs-cluster.cc)
target_link_libraries(start-external-mini-rs-cluster ${LINK_LIBS})

add_executable(client-test client-test.cc)
target_link_libraries(client-test ${LINK_LIBS})
add_test(client-test ${EXECUTABLE_OUTPUT_PATH}/client-test)

add_executable(external-mini-cluster-test external-mini-cluster-test.cc)
target_link_libraries(external-mini-cluster-test ${LINK_LIBS})
add_test(external-mini-cluster-test ${EXECUTABLE_OUTPUT_PATH}/external-mini-cluster-test)

add_library(ExternalMiniCluster SHARED start-external-mini-rs-cluster.cc)
target_link_libraries(ExternalMiniCluster ${LINK_LIBS})
