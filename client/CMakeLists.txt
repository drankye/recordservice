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

cmake_minimum_required(VERSION 2.6)

if (APPLE)
  message(STATUS "Building CPP library not currently supported.")
elseif (UNIX)
  SET(BUILD_CPP True)
endif()

IF (DEFINED ENV{IMPALA_TOOLCHAIN})
  message(STATUS "Toolchain enabled. Building with toolchain compiler.")
  message(STATUS "Toolchain home: $ENV{IMPALA_TOOLCHAIN}")
  set(GCC_ROOT $ENV{IMPALA_TOOLCHAIN}/gcc-4.9.2)
  set(CMAKE_C_COMPILER ${GCC_ROOT}/bin/gcc)
  set(CMAKE_CXX_COMPILER ${GCC_ROOT}/bin/g++)
  message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")
ENDIF (DEFINED ENV{IMPALA_TOOLCHAIN})

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(${THRIFT_INCLUDE_DIR})
set(LIBS ${LIBS} ${THRIFT_LIBS})
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift contrib dir: ${THRIFT_CONTRIB_DIR}")
message(STATUS "Thrift library path: ${THRIFT_LIBS}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
# for static linking with Thrift, THRIFT_STATIC_LIB is set in FindThrift.cmake
add_library(thriftstatic STATIC IMPORTED)
set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

if (BUILD_CPP)
  # find GFlags headers and libs (needed for GLog)
  find_package(GFlags REQUIRED)
  include_directories(${GFLAGS_INCLUDE_DIR})
  set(LIBS ${LIBS} ${GFLAGS_LIBS})
  # for static linking with GFLAGS, GFLAGS_STATIC_LIB is set in GFLAGS' find module
  add_library(gflagsstatic STATIC IMPORTED)
  set_target_properties(gflagsstatic PROPERTIES IMPORTED_LOCATION ${GFLAGS_STATIC_LIB})
  message(STATUS "GFlags include dir: " ${GFLAGS_INCLUDE_DIR})

  # find GTest headers and libs
  find_package(GTest REQUIRED)
  include_directories(${GTEST_INCLUDE_DIR})
  set(LIBS ${LIBS} ${GTEST_LIBRARIES})
  add_library(gtest STATIC IMPORTED)
  set_target_properties(gtest PROPERTIES IMPORTED_LOCATION "${GTEST_LIBRARY}")
  message(STATUS "GTest include dir: " ${GTEST_INCLUDE_DIR})
  message(STATUS "GTest library: " ${GTEST_LIBRARY})

  # find jni headers and libs
  find_package(JNI REQUIRED)
  include_directories(${JNI_INCLUDE_DIRS})
  set(LIBS ${LIBS} ${JNI_LIBRARIES})
  message(STATUS "JNI_INCLUDE_DIRS: ${JNI_INCLUDE_DIRS}")
  message(STATUS "JNI_LIBRARIES: ${JNI_LIBRARIES}")
endif()

# compiler flags that are common across debug/release builds
#  - msse4.2: Enable sse4.2 compiler intrinsics.
#  - Wall: Enable all warnings.
#  - Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#   -fno-strict-aliasing: disable optimizations that assume strict aliasing. This
#       is unsafe to do if the code uses casts (which we obviously do).
#  - Wno-unknown-pragmas: suppress warnings for unknown (compiler specific) pragmas
#  - Wno-deprecated: gutil contains deprecated headers
#  - pthread: enable multithreaded malloc
#  - DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
SET(CXX_COMMON_FLAGS "-msse4.2 -Wall -Wno-sign-compare -Wno-unknown-pragmas -pthread")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-strict-aliasing")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated -Wno-unused-local-typedefs")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DHAVE_NETDB_H)

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -g: Enable symbols for profiler tools
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
SET(CXX_FLAGS_DEBUG "-g -ggdb")
SET(CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG")
SET(CXX_FLAGS_ADDRESS_SANITIZER
    "-O1 -g -fsanitize=address -fno-omit-frame-pointer -DADDRESS_SANITIZER")

# if no build build type is specified, default to release builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif(NOT CMAKE_BUILD_TYPE)

STRING (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  SET(CMAKE_CXX_COMPILER ${LLVM_CLANG_EXECUTABLE})
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_ADDRESS_SANITIZER})
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
MESSAGE(STATUS, "Compiler Flags: ${CMAKE_CXX_FLAGS}")

# set compile output directory
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_DEBUG" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/cpp/build/debug/")
else()
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/cpp/build/release/")
endif()

include_directories($ENV{RECORD_SERVICE_HOME}/cpp/generated-sources)

# allow linking of static libs into dynamic lib
add_definitions(-fPIC)

# compile these subdirs using their own CMakeLists.txt
add_subdirectory(api)

if (BUILD_CPP)
  add_subdirectory(cpp/samples)
  add_subdirectory(cpp/tests)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable")
  add_subdirectory(cpp/generated-sources/gen-cpp)
endif()

# where to put generated libraries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")
