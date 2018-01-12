// Copyright 2012 Cloudera Inc.
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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import com.cloudera.recordservice.core.NetworkAddress;

/**
 * Config keys and values for the RecordService and utilities to set them.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RecordServiceConfig {
  static {
    Configuration.addDefaultResource("recordservice-site.xml");
  }

  // All RecordService configuration properties, in the format of:
  // (property name, property description)
  public enum ConfVars {
    QUERY_NAME_CONF("recordservice.query", "The query to generate records from"),
    TBL_NAME_CONF("recordservice.table.name", "The fully qualified table name to read"),
    COL_NAMES_CONF("recordservice.col.names", "The subset of columns to read"),
    PLANNER_HOSTPORTS_CONF("recordservice.planner.hostports",
        "Host/Port of the planner service. Comma separated list."),
    KERBEROS_PRINCIPAL_CONF("recordservice.kerberos.principal", "Kerberos principal"),
    // TODO: It would be nice for the server to adjust this automatically based
    // on how fast the client is able to process records.
    FETCH_SIZE_CONF("recordservice.task.fetch.size",
        "Optional configuration option for performance tuning that configures the" +
            " max number of records returned when fetching results from the " +
            " RecordService. If not set, server default will be used."),
    MEM_LIMIT_CONF("recordservice.task.memlimit.bytes",
        "Optional configuration for the maximum memory the server will use per task"),
    RECORDS_LIMIT_CONF("recordservice.task.records.limit",
        "Optional configuration for the maximum number of records returned per task"),
    PLANNER_REQUEST_MAX_TASKS("recordservice.task.plan.maxTasks",
        "Optional configuration for the hinted maximum number of tasks to generate " +
            "per PlanRequest"),
    PLANNER_RETRY_ATTEMPTS_CONF("recordservice.planner.retry.attempts",
        "Optional configuration for the maximum number of attempts to retry " +
            "RecordService RPCs with planner"),
    WORKER_RETRY_ATTEMPTS_CONF("recordservice.worker.retry.attempts",
        "Optional configuration for the maximum number of attempts to retry" +
            " RecordService RPCs with worker."),
    PLANNER_RETRY_SLEEP_MS_CONF("recordservice.planner.retry.sleepMs",
        "Optional configuration for sleep between retry attempts with planner"),
    WORKER_RETRY_SLEEP_MS_CONF("recordservice.worker.retry.sleepMs",
        "Optional configuration for sleep between retry attempts with worker"),
    PLANNER_RPC_TIMEOUT_MS_CONF("recordservice.planner.rpc.timeoutMs",
        "Optional configuration for timeout for planner RPCs"),
    WORKER_RPC_TIMEOUT_MS_CONF("recordservice.worker.rpc.timeoutMs",
        "Optional configuration for timeout for planner RPCs"),
    PLANNER_CONNECTION_TIMEOUT_MS_CONF("recordservice.planner.connection.timeoutMs",
        "Optional configuration for timeout when connecting to the worker service"),
    WORKER_CONNECTION_TIMEOUT_MS_CONF("recordservice.worker.connection.timeoutMs",
        "Optional configuration for timeout when connecting to the worker service"),
    WORKER_ENABLE_SERVER_LOGGING_CONF("recordservice.worker.server.enableLogging",
        "Optional configuration to enable server logging (logging level from log4j)"),
    ZOOKEEPER_CONNECTION_STRING_CONF("recordservice.zookeeper.connectString",
        "ZooKeeper related configurations - these are optional and only used if " +
            "planner auto discovery is enabled"),
    ZOOKEEPER_CONNECT_TIMEOUTMILLIS_CONF("recordservice.zookeeper.connectTimeoutMillis",
        "ZK connection timeout (in milli seconds)"),
    ZOOKEEPER_ZNODE_CONF("recordservice.zookeeper.znode", "Root zookeeper directory");

    public final String name;
    public final String desc;

    // TODO: more fields (default value, types, etc)
    ConfVars(String name, String desc) {
      this.name = name;
      this.desc = desc;
    }
  }

  // TODO: define getter/setters for ConfVars. Right now users have to explicitly refer
  // to the "name" field. Probably better to hide the detail.

  public static final String DEFAULT_PLANNER_HOSTPORTS =
    System.getenv("RECORD_SERVICE_PLANNER_HOST") != null ?
        System.getenv("RECORD_SERVICE_PLANNER_HOST") + ":12050" : "localhost:12050";

  public static final String ZOOKEEPER_ZNODE_DEFAULT = "/recordservice";

  /**
   * Set the request type based on the input string.
   * - starts with "select" - assume it is a query
   * - starts with "/" - assume it is a path
   * - starts with "hdfs:/" - assume it is a path
   * - starts with "s3a:/" - assume it is a path
   * - otherwise, assume it is a db.table
   */
  public static void setInput(Configuration config, String input) throws IOException {
    if (input.toLowerCase().startsWith("select")) {
      setInputQuery(config, input);
    } else if (input.startsWith("/")) {
      setInputPaths(config, new Path(input));
    } else if (input.toLowerCase().startsWith("hdfs:/")) {
      setInputPaths(config, new Path(input));
    } else if (input.toLowerCase().startsWith("s3a:/")) {
      setInputPaths(config, new Path(input));
    } else {
      setInputTable(config, null, input);
    }
  }

  /**
   * Sets the input configuration to read 'cols' from 'db.tbl'. If the tbl is fully
   * qualified, db should be null.
   * If cols is empty, all cols in the table are read.
   */
  public static void setInputTable(Configuration conf, String db, String tbl,
      String... cols) {
    if (tbl == null || tbl == "") {
      throw new IllegalArgumentException("'tbl' must be non-empty");
    }
    if (db != null && db != "") tbl = db + "." + tbl;
    conf.set(ConfVars.TBL_NAME_CONF.name, tbl);
    if (cols != null && cols.length > 0) {
      for (int i = 0; i < cols.length; ++i) {
        if (cols[i] == null || cols[i].isEmpty()) {
          throw new IllegalArgumentException("Column list cannot contain empty names.");
        }
      }
      conf.setStrings(ConfVars.COL_NAMES_CONF.name, cols);
    }
  }

  /**
   * Sets the input configuration to read results from 'query'.
   */
  public static void setInputQuery(Configuration conf, String query) {
    if (query == null)  throw new IllegalArgumentException("'query' must be non-null");
    conf.set(ConfVars.QUERY_NAME_CONF.name, query);
  }

  /**
   * Set the array of {@link Path}s as the list of inputs
   * for the map-reduce job.
   */
  public static void setInputPaths(Configuration conf,
      Path... inputPaths) throws IOException {
    Path path = inputPaths[0].getFileSystem(conf).makeQualified(inputPaths[0]);
    StringBuffer str = new StringBuffer(StringUtils.escapeString(path.toString()));
    for(int i = 1; i < inputPaths.length; ++i) {
      str.append(StringUtils.COMMA_STR);
      path = inputPaths[i].getFileSystem(conf).makeQualified(inputPaths[i]);
      str.append(StringUtils.escapeString(path.toString()));
    }
    conf.set("mapred.input.dir", str.toString());
  }


  /**
   * Returns a random hostport from plannerHostPortsStr. Each hostport should be
   * comma separated.
   */
  public static List<NetworkAddress> getPlannerHostPort(String plannerHostPortsStr)
      throws IOException {
    String[] plannerHostPorts = plannerHostPortsStr.trim().split(",");
    if (plannerHostPorts.length == 0) {
      throw new IOException("Invalid planner host port list: " + plannerHostPortsStr);
    }
    List<NetworkAddress> result = new ArrayList<NetworkAddress>();
    for (String hostPortStr: plannerHostPorts) {
      if (hostPortStr.length() == 0) continue;
      String[] hostPort = hostPortStr.split(":");
      if (hostPort.length != 2) {
        throw new IOException("Invalid hostport: " + hostPortStr);
      }
      String host = hostPort[0];
      int port = 0;
      try {
        port = Integer.parseInt(hostPort[1]);
      } catch (NumberFormatException e) {
        throw new IOException("Invalid hostport: " + hostPortStr);
      }
      result.add(new NetworkAddress(host, port));
    }
    return result;
  }
}
