#!/usr/bin/env impala-python
# Copyright (c) 2015, Cloudera, inc.
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

import sh
import os
import sys
import re

HOST = "http://unittest.jenkins.cloudera.com/job/verify-impala-toolchain-package-build/"
SOURCE = "http://github.mtv.cloudera.com/mgrund/impala-deps/raw/master"
BUILD = "63"

OS_MAPPING = {
  "centos6" : "ec2-package-centos-6",
  "centos6.4" : "ec2-package-centos-6",
  "centos6.5" : "ec2-package-centos-6",
  "centos5" : "ec2-package-centos-5",
  "centos7" : "ec2-package-centos-7",
  "debian6" : "ec2-package-debian-6",
  "debian7" : "ec2-package-debian-7",
  "debian7.1" : "ec2-package-debian-7",
  "sles11" : "ec2-pacage-sles-11",
  "ubuntu12.04" : "ec2-package-ubuntu-12-04",
  "ubuntu14.04" : "ec2-package-ubuntu-14-04"
}

def map_release_label():
 """Gets the right package label from the OS version"""
 release = "".join(map(lambda x: x.lower(), sh.lsb_release("-irs").split()))
 return OS_MAPPING[next(k for k in OS_MAPPING if re.search(k, release))]


def download_package(name, destination, compiler=""):
  label = map_release_label()
  if len(compiler) > 0:
    compiler = "-" + compiler
  url = "{0}/{1}/label={2}/artifact/toolchain/build/{3}{4}.tar.gz".format(
    HOST, BUILD, label, name, compiler)

  # Download the file
  print "Downloading {0}".format(name)
  sh.wget(url, directory_prefix=destination, no_clobber=True)
  # Extract
  print "Extracting {0}".format(name)
  sh.tar(z=True, x=True, f="{0}/{1}{2}.tar.gz".format(destination, name, compiler),
         directory=destination)
  sh.rm("{0}/{1}{2}.tar.gz".format(destination, name, compiler))


def bootstrap(packages, destination=None, compiler=None):
  """Bootstrap will create a bootstrap directory within $IMPALA_HOME and download and
  install the necessary packages and pre-built binaries."""
  path = os.getenv("IMPALA_HOME", None)
  if path is None:
    print "Impala environment not setup correctly, make sure $IMPALA_HOME is present."

  if destination is None:
    destination = os.path.join(path, "toolchain", "build")

  if compiler is None:
    compiler = "gcc-" + os.getenv("IMPALA_GCC_VERSION")

  # Create the destination directory if necessary
  if not os.path.exists(destination):
    os.makedirs(destination)

  for p in packages:
    download_package(p, destination, compiler)

def get_version(name):
  return name + "-" + os.getenv("IMPALA_" + name.upper() + "_VERSION")

if __name__ == "__main__":
  compiler = get_version("gcc")
  packages = [
                get_version("avro"),
                get_version("boost"),
                get_version("breakpad"),
                get_version("bzip2"),
                "cyrus-sasl-" + os.getenv("IMPALA_CYRUS_SASL_VERSION"),
                get_version("gcc"),
                get_version("gflags"),
                get_version("glog"),
                get_version("gperftools"),
                get_version("gtest"),
                get_version("llvm"),
                "llvm-trunk",
                get_version("lz4"),
                get_version("openldap"),
                get_version("rapidjson"),
                get_version("re2"),
                get_version("snappy"),
                get_version("thrift"),
                get_version("zlib"),
             ]

  bootstrap(packages, compiler=compiler)
