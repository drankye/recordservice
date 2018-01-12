// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.cloudera.recordservice.core;

import java.io.File;
import java.io.IOException;

/**
 * Common utilities to use for testing.
 */
public class TestUtil {
  /**
   * Returns the absolute path of a temp directory. The directory is *not* created.
   */
  public static String getTempDirectory() throws IOException {
    final File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    if (!(temp.delete())) {
      throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
    }
    return temp.getAbsolutePath();
  }

  /**
   * Get a Kerberos principal corresponding to the 'hostName'.
   * For instance: given "impala" and "rs-kerberos-1.vpc.cloudera.com" this returns
   * "impala/rs-kerberos-1.vpc.cloudera.com/VPC.CLOUDERA.COM"
   */
  public static String makePrincipal(String primary, String hostName) {
    int firstDot = hostName.indexOf(".");
    return makePrincipal(primary, hostName,
        hostName.substring(firstDot + 1).toUpperCase());
  }

  /**
   * Get a Kerberos principal corresponding to the 'primary', 'hostName' and 'realm'.
   */
  public static String makePrincipal(String primary, String hostName, String realm) {
    return primary + "/" + hostName + "@" + realm;
  }
}
