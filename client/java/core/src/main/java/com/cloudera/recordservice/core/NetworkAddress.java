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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.recordservice.thrift.TNetworkAddress;

/**
 * POJO for TNetworkAddress
 */
public class NetworkAddress implements Serializable {
  private static final long serialVersionUID = 1423199708868084152L;

  public final String hostname;
  public final int port;

  public NetworkAddress(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public String toString() {
    return hostname + ":" + port;
  }

  NetworkAddress(TNetworkAddress address) {
    hostname = address.hostname;
    port = address.port;
  }

  /**
   * Returns a list of LogMessages from the thrift version.
   */
  static List<NetworkAddress> fromThrift(List<TNetworkAddress> list) {
    List<NetworkAddress> result = new ArrayList<NetworkAddress>();
    for (TNetworkAddress l: list) {
      result.add(new NetworkAddress(l));
    }
    return result;
  }
}
