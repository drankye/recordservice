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

set (LINK_LIBS RecordServiceThrift thriftstatic gflagsstatic TestUtil)

add_executable(record-service-client record-service-client.cc)
target_link_libraries(record-service-client ${LINK_LIBS} -lboost_date_time -lrt)

add_executable(rs-grep rs-grep.cc)
target_link_libraries(rs-grep ${LINK_LIBS})

add_executable(plan-request plan-request.cc)
target_link_libraries(plan-request ${LINK_LIBS})
