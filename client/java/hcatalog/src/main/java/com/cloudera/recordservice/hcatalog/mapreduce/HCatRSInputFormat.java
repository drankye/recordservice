/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cloudera.recordservice.hcatalog.mapreduce;

import java.io.IOException;
import java.util.List;

import com.cloudera.recordservice.hcatalog.common.HCatRSUtil;
import com.cloudera.recordservice.mr.PlanUtil;
import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.Pair;
import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.mr.security.DelegationTokenIdentifier;
import com.cloudera.recordservice.mr.security.TokenUtils;
import com.cloudera.recordservice.mr.RecordServiceConfig.ConfVars;

/**
 * The InputFormat to use to read data from HCatalog.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatRSInputFormat extends HCatRSBaseInputFormat {
  private InputJobInfo inputJobInfo;

  /**
   * Initializes the input with a provided filter.
   * See {@link #setInput(Configuration, String, String, String)}
   */
  public static HCatRSInputFormat setInput(Job job, String location, String filter)
      throws IOException {
    Configuration conf = job.getConfiguration();
    String kerberosPrincipal = conf.get(ConfVars.KERBEROS_PRINCIPAL_CONF.name);
    Pair<String, String> dbTablePair = HCatUtil.getDbAndTableName(location);
    dbTablePair = HCatRSUtil.cleanQueryPair(dbTablePair);
    String dbName = dbTablePair.first;
    String tableName = dbTablePair.second;
    if (location.toLowerCase().startsWith("select")) {
      RecordServiceConfig.setInputQuery(conf, location);
    }
    else{
      RecordServiceConfig.setInputTable(conf, dbName, tableName);
    }
    Credentials credentials = job.getCredentials();
    RecordServicePlannerClient.Builder builder = PlanUtil.getBuilder(conf);
    List<NetworkAddress> plannerHosts = PlanUtil.getPlannerHostPorts(conf);
    RecordServicePlannerClient planner = null;
    try {
      planner = PlanUtil.getPlanner(
          conf, builder, plannerHosts, kerberosPrincipal, credentials);
      if (planner.isKerberosAuthenticated()) {
        Token<DelegationTokenIdentifier> delegationToken
            = TokenUtils.fromTDelegationToken(planner.getDelegationToken(""));
        credentials.addToken(DelegationTokenIdentifier.DELEGATION_KIND, delegationToken);
      }
    } catch (RecordServiceException e) {
      throw new IOException(e);
    } finally {
      if (planner != null) planner.close();
    }
    job.setInputFormatClass(HCatRSInputFormat.class);
    return setInput(conf, dbName, tableName, filter);
  }

  /**
   * Set inputs to use for the job. This queries the metastore with the given input
   * specification and serializes matching partitions into the job conf for use by
   * MR tasks.
   * @param conf the job configuration
   * @param dbName database name, which if null 'default' is used
   * @param tableName table name
   * @param filter the partition filter to use, can be null for no filter
   * @throws IOException on all errors
   */
  public static HCatRSInputFormat setInput(
          Configuration conf, String dbName, String tableName, String filter)
    throws IOException {
    Preconditions.checkNotNull(conf, "required argument 'conf' is null");
    Preconditions.checkNotNull(tableName, "required argument 'tableName' is null");

    HCatRSInputFormat hCatRSInputFormat = new HCatRSInputFormat();
    hCatRSInputFormat.inputJobInfo = InputJobInfo.create(dbName, tableName, filter, null);

    try {
      InitializeInput.setInput(conf, hCatRSInputFormat.inputJobInfo);
    } catch (Exception e) {
      throw new IOException(e);
    }

    return hCatRSInputFormat;
  }

}
