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

import java.util.Arrays;
import java.util.List;

/**
 * This is part of the client interface.
 */
public class ProtocolVersion {
  public static final String CLIENT_VERSION = "1.0";

  // White list for server protocol version
  private static final List<String> SERVER_VERSION_LIST = Arrays.asList("1.0");

  private String version;

  ProtocolVersion(String version) {
    this.version = version;
  }

  public String getVersion() {
    return this.version;
  }

  /**
   * Return true if the server version is within the server version list.
   */
  public boolean isValidProtocolVersion() {
    return SERVER_VERSION_LIST.contains(version);
  }
}
