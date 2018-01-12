// Copyright 2016 Cloudera Inc.
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

package com.cloudera.recordservice.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.mr.RecordServiceConfig.ConfVars;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for interacting with ZooKeeper.
 * The main method is {@link #getPlanners(org.apache.hadoop.conf.Configuration)}, which
 * can be used to query ZooKeeper and get a list of planners that are running.
 */
public class ZooKeeperUtil {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ZooKeeperUtil.class.getName());

  private static final List<ACL> readAcl = ZooDefs.Ids.READ_ACL_UNSAFE;

  /**
   * Returns a list of network addresses for the RecordService planners currently
   * available as maintained by ZooKeeper.
   * @param conf The input client job configuration
   * @return A list of <code>NetworkAddress</code>es for all the planners available
   */
  public static List<NetworkAddress> getPlanners(Configuration conf) throws IOException {
    String connectionString = conf.get(ConfVars.ZOOKEEPER_CONNECTION_STRING_CONF.name);
    if (connectionString == null || connectionString.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "Zookeeper connect string has to be specified through "
              + ConfVars.ZOOKEEPER_CONNECTION_STRING_CONF.name);
    }
    LOGGER.info("Connecting to zookeeper at: " + connectionString);

    int connectionTimeout =
        conf.getInt(ConfVars.ZOOKEEPER_CONNECT_TIMEOUTMILLIS_CONF.name,
            CuratorFrameworkFactory.builder().getConnectionTimeoutMs());
    LOGGER.info("Zookeeper connection timeout: " + connectionTimeout);


    String rootNode = conf.get(ConfVars.ZOOKEEPER_ZNODE_CONF.name,
        RecordServiceConfig.ZOOKEEPER_ZNODE_DEFAULT);
    LOGGER.info("Zookeeper root: " + rootNode);

    CuratorFramework cf = CuratorFrameworkFactory.builder()
        .connectString(connectionString)
        .connectionTimeoutMs(connectionTimeout)
        .aclProvider(new ZooKeeperACLProvider())
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .build();
    cf.start();

    List<NetworkAddress> result = new ArrayList<NetworkAddress>();
    try {
      for (String path : cf.getChildren().forPath(rootNode + "/planners")) {
        NetworkAddress addr = parsePath(path);
        if (addr != null) result.add(parsePath(path));
      }
    } catch (Exception e) {
      cf.close();
      throw new IOException("Could not obtain planner membership" +
          " from " + connectionString + ". Error message: " + e.getMessage(), e);
    }
    cf.close();
    return result;
  }

  /**
   * Parse ZK path string in the format of
   *   <code>/recordservice/planners/recordserviced@hostname:port</code>
   * to a corresponding <code>NetWorkAddress</code>
   * Returns null if the path is of illegal format.
   */
  private static NetworkAddress parsePath(String path) {
    String hostname;
    int port;
    try {
      int atIdx = path.indexOf('@');
      if (atIdx < 0) {
        throw new IOException("Couldn't locate '@' in path: " + path);
      }
      int colonIdx = path.indexOf(':', atIdx + 1);
      if (colonIdx < 0) {
        throw new IOException("Couldn't locate ':' in path: " + path);
      }
      hostname = path.substring(atIdx + 1, colonIdx);
      try {
        port = Integer.parseInt(path.substring(colonIdx + 1, path.length()));
      } catch (NumberFormatException e) {
        throw new IOException("Couldn't parse port number in path: " + path, e);
      }
    } catch (IOException e) {
      LOGGER.warn("Error while parsing input path", e);
      return null;
    }
    return new NetworkAddress(hostname, port);
  }

  /**
   * An ACLProvider class that is used for accessing ZooKeeper data.
   */
  private static class ZooKeeperACLProvider implements ACLProvider {
    @Override
    public List<ACL> getDefaultAcl() {
      return readAcl;
    }
    @Override
    public List<ACL> getAclForPath(String s) {
      return getDefaultAcl();
    }
  }
}
