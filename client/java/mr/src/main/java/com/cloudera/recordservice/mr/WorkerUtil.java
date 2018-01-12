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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

import com.cloudera.recordservice.core.DelegationToken;
import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceWorkerClient;
import com.cloudera.recordservice.core.RecordServiceWorkerClient.Builder;
import com.cloudera.recordservice.core.UniqueId;
import com.cloudera.recordservice.mr.RecordServiceConfig.ConfVars;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for RecordService worker client
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WorkerUtil {

  private final static Logger LOG = LoggerFactory.getLogger(WorkerUtil.class);

  // Default fetch size to use for MR and Spark. This is currently much larger than the
  // server default but perfs better this way (but uses more memory).
  // TODO: investigate this more and do this in the server. Remove this.
  private static final int DEFAULT_FETCH_SIZE = 50000;

  /**
   * Creates a builder for RecordService worker client from the configuration and
   * the delegation token.
   * @param jobConf the hadoop configuration
   * @param delegationToken the delegation token that the worker client should use to
   *                        talk to the RS worker process.
   * @throws IOException
   */
  public static Builder getBuilder(Configuration jobConf,
                                   DelegationToken delegationToken) {
    // Try to get the delegation token from the credentials. If it is there, use it.
    RecordServiceWorkerClient.Builder builder =
        new RecordServiceWorkerClient.Builder();
    int fetchSize = jobConf.getInt(ConfVars.FETCH_SIZE_CONF.name, DEFAULT_FETCH_SIZE);
    long memLimit = jobConf.getLong(ConfVars.MEM_LIMIT_CONF.name, -1);
    long limit = jobConf.getLong(ConfVars.RECORDS_LIMIT_CONF.name, -1);
    int maxAttempts = jobConf.getInt(ConfVars.WORKER_RETRY_ATTEMPTS_CONF.name, -1);
    int taskSleepMs = jobConf.getInt(ConfVars.WORKER_RETRY_SLEEP_MS_CONF.name, -1);
    int connectionTimeoutMs = jobConf.getInt(
        ConfVars.WORKER_CONNECTION_TIMEOUT_MS_CONF.name, -1);
    int rpcTimeoutMs = jobConf.getInt(ConfVars.WORKER_RPC_TIMEOUT_MS_CONF.name, -1);
    boolean enableLogging =
        jobConf.getBoolean(ConfVars.WORKER_ENABLE_SERVER_LOGGING_CONF.name, false);

    if (fetchSize != -1) builder.setFetchSize(fetchSize);
    if (memLimit != -1) builder.setMemLimit(memLimit);
    if (limit != -1) builder.setLimit(limit);
    if (maxAttempts != -1) builder.setMaxAttempts(maxAttempts);
    if (taskSleepMs != -1 ) builder.setSleepDurationMs(taskSleepMs);
    if (connectionTimeoutMs != -1) builder.setConnectionTimeoutMs(connectionTimeoutMs);
    if (rpcTimeoutMs != -1) builder.setRpcTimeoutMs(rpcTimeoutMs);
    if (enableLogging) builder.setLoggingLevel(LOG);
    if (delegationToken != null) builder.setDelegationToken(delegationToken);

    return builder;
  }

  /**
   * Returns the worker host to connect to, attempting to schedule for locality. If
   * any of the hosts matches this hosts address, use that host. Otherwise connect
   * to a host at random.
   * @param taskId the RecordService task Id. Used for logging.
   * @param localLocations locations where both data and worker are available
   * @param globalLocations locations where worker is available
   * @return a network address to schedule the task to
   */
  public static NetworkAddress getWorkerToConnectTo(
      UniqueId taskId,
      List<NetworkAddress> localLocations,
      List<NetworkAddress> globalLocations) throws UnknownHostException {
    NetworkAddress address = null;
    // Important! We match locality on host names, not ips.
    String localHost = InetAddress.getLocalHost().getHostName();

    // 1. If the data is available on this node, schedule the task locally.
    for (NetworkAddress loc : localLocations) {
      if (localHost.equals(loc.hostname)) {
        LOG.info("Both data and RecordServiceWorker are available locally for task {}",
            taskId);
        address = loc;
        break;
      }
    }

    // 2. Check if there's a RecordServiceWorker running locally. If so, pick that node.
    if (address == null) {
      for (NetworkAddress loc : globalLocations) {
        if (localHost.equals(loc.hostname)) {
          address = loc;
          LOG.info("RecordServiceWorker is available locally for task {}.", taskId);
          break;
        }
      }
    }

    // 3. Finally, we don't have RecordServiceWorker running locally. Randomly pick
    // a node from the global membership.
    if (address == null) {
      Random rand = new Random();
      address = globalLocations.get(rand.nextInt(globalLocations.size()));
      LOG.info("Neither RecordServiceWorker nor data is available locally for task {}." +
          " Randomly selected host {} to execute it", taskId, address.hostname);
    }

    return address;
  }

}
