// Copyright 2015 Cloudera Inc.
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

package com.cloudera.recordservice.tests;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONException;

/**
 * This class controls the cluster during tests, both the minicluster and a real
 * cluster. To control the minicluster commands are run through the
 * MiniClusterController class but using this class as an api allows the tests
 * to be cluster agnostic.
 */
public class ClusterController {
  public static final int DEFAULT_NUM_NODES = 3;
  public static final String CM_USER_NAME = "admin";
  public static final String CM_PASSWORD = "admin";

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterController.class);

  public static ClusterController cluster_;

  public final boolean USE_MINI_CLUSTER;
  public final String RECORD_SERVICE_PLANNER_HOST;

  public ClusterConfiguration clusterConfiguration_;
  public List clusterList_;
  public List activeNodes_;
  public List availableNodes_;
  public String HADOOP_CONF_DIR;

  /**
   * If a miniCluster is being used, this class simply instantiates a
   * MiniClusterController.
   *
   * If a real cluster is being used, this class gets the necessary
   * configuration files via the CM api. The HADOOP_CONF_DIR and
   * RECORD_SERVICE_PLANNER_HOSTS environment variables are set. These only
   * apply within the JVM.
   *
   * Variables: boolean miniCluster: if true, use miniCluster String hostname:
   * the hostname of a CM enabled machine in a cluster
   *
   * TODO: Future work involves doing the necessary steps to ensure that a
   * cluster is healthy and ready for RecordService jobs to be executed.
   */
  public ClusterController(boolean miniCluster, String hostname) {
    USE_MINI_CLUSTER = miniCluster;
    RECORD_SERVICE_PLANNER_HOST = hostname;
    try {
      if (USE_MINI_CLUSTER) {
        cluster_ = MiniClusterController.instance();
        HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");
      } else {
        clusterConfiguration_ = new ClusterConfiguration(hostname, CM_USER_NAME,
            CM_PASSWORD);
        Map<String, String> envMap = new HashMap<String, String>();
        HADOOP_CONF_DIR = clusterConfiguration_.getHadoopConfDir();
        String SERVER_HOME = System.getenv("SERVER_HOME");
        String RECORD_SERVICE_HOME = System.getenv("RECORD_SERVICE_HOME");
        envMap.put("HADOOP_CONF_DIR", HADOOP_CONF_DIR);
        envMap.put("RECORD_SERVICE_PLANNER_HOST", RECORD_SERVICE_PLANNER_HOST);
        envMap.put("RECORD_SERVICE_HOME", RECORD_SERVICE_HOME);
        envMap.put("SERVER_HOME", SERVER_HOME);
        envMap.put("HADOOP_HOME", System.getenv("HADOOP_HOME"));
        // Add these two additional system variables to the JVM environment.
        // Hadoop and RecordService rely on these variables to execute on a
        // cluster.
        setEnv(envMap);
        LOGGER.debug("HADOOP_CONF_DIR: " + System.getenv("HADOOP_CONF_DIR"));
        LOGGER.debug("HADOOP_HOME: " + System.getenv("HADOOP_HOME"));
        cluster_ = this;
      }
    } catch (MalformedURLException e) {
      LOGGER.debug("Error getting cluster configuration", e);
      System.exit(1);
    } catch (JSONException e) {
      LOGGER.debug("Error getting cluster configuration", e);
      System.exit(1);
    } catch (IOException e) {
      LOGGER.debug("Error getting cluster configuration", e);
      System.exit(1);
    }
  }

  /**
   * This method runs the given job as specified in the JobConf on the cluster
   */
  public RunningJob runJob(JobConf mrJob) throws IOException {
    return JobClient.runJob(mrJob);
  }

  /**
   * This method adds a node to the cluster. In the case of the minicluster this
   * is a very straightforward procedure, a recordserviced is brought up.
   *
   * TODO: Future work is required to make this method work on a real cluster.
   * In that case this method would add a node that was already in the cluster
   * but had previously been disabled. If there were no such node, this method
   * would not do anything.
   */
  public void addNode() {
    throw new NotImplementedException();
  }

  /**
   * This method returns a JobConf object that allows a map reduce job to be run
   * on the cluster
   */
  public JobConf getJobConf() throws MalformedURLException {
    JobConf conf = new JobConf();
    populateJobConf(conf);
    return conf;
  }

  /**
   * This method populates a JobConf with the information in the HadoopConfDir
   */
  public JobConf populateJobConf(JobConf conf) throws MalformedURLException {
    File[] files = new File(clusterConfiguration_.getHadoopConfDir()).listFiles();
    for (File file : files) {
      if (file.getName().endsWith(".xml")) {
        conf.addResource(file.getAbsoluteFile().toURI().toURL());
      }
    }
    String[] bs = clusterConfiguration_.getHadoopConfDir().split("/");
    String newPath = "/";
    for (int i = 0; i < bs.length - 1; i++) {
      newPath += bs[i] + "/";
    }
    newPath += "recordservice-conf/recordservice-site.xml";
    conf.addResource(new File(newPath).getAbsoluteFile().toURI().toURL());
    return conf;
  }

  /**
   * This method allows the caller to add environment variables to the JVM.
   * There is no easy way to do this through a simple call, such as there is to
   * read env variables using System.getEnv(variableName). Much of the method
   * was written with guidance from stack overflow:
   * http://stackoverflow.com/questions
   * /318239/how-do-i-set-environment-variables-from-java
   */
  protected static void setEnv(Map<String, String> newenv) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass
          .getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
          .getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField
          .get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception e2) {
        e2.printStackTrace();
      }
    } catch (Exception e1) {
      e1.printStackTrace();
    }
  }

  /**
   * This class represents a node in a cluster. It contains basic information
   * such as hostname and open ports.
   */
  public static class ClusterNode {
    public String hostname_;
    public int workerPort_;
    public int plannerPort_;
    public int webserverPort_;

    public ClusterNode(String hostname) {
      hostname_ = hostname;
    }

    public ClusterNode(String hostname, int workerPort, int plannerPort, int webserverPort) {
      hostname_ = hostname;
      workerPort_ = workerPort;
      plannerPort_ = plannerPort;
      webserverPort_ = webserverPort;
    }
  }
}
