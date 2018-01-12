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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.cloudera.recordservice.util.Preconditions;

/**
 * A class that starts a minicluster locally and runs a map reduce job on the
 * cluster through JNI calls.
 *
 * This class is a singleton. It first needs to be instantiated, and then the
 * instance method returns the class instance.
 *
 * Usage:
 * MiniClusterController.Start(num_nodes);
 * MiniClusterController miniCluster = MiniClusterController.instance();
 */
public class MiniClusterController extends ClusterController {
  private static native void StartMiniCluster(int numNodes);

  private native void KillNodeByPid(int pid);
  private native int[] GetRunningMiniNodePids();
  private native int GetSpecificNodePid(int plannerPort);
  private native int AddRecordServiceNode(boolean startPlanner, boolean startWorker);
  private native String[] GetNodeArgs(int pid);

  private static final String MINICLUSTER_LIBRARY = "libExternalMiniCluster.so";
  private static final String BUILT_RS_CODE_LOCATION = "/cpp/build/release/recordservice/";

  private Thread clusterThread_;
  private List<MiniClusterNode> clusterList_;

  private static MiniClusterController miniCluster_;

  /**
   * This method starts a minicluster with the specified number of nodes. It
   * should only be called once. If the minicluster has already been
   * instantiated, nothing is executed.
   */
  public static void Start(int numNodes) throws InterruptedException {
    new MiniClusterController(numNodes);
  }

  /**
   * This method returns the instantiated instance of MiniClusterController. If
   * the MiniClusterController has not been instantiated, it returns null.
   */
  public static MiniClusterController instance() {
    return miniCluster_;
  }

  /**
   * Every node in the local minicluster runs in its own process. This method
   * returns a hashset of all the process ids of running nodes in the mini
   * cluster.
   */
  public HashSet<Integer> getRunningMiniNodePids() {
    HashSet<Integer> pidSet = new HashSet<Integer>();
    Collections.addAll(pidSet, ArrayUtils.toObject(GetRunningMiniNodePids()));
    return pidSet;
  }

  /**
   * This method kills the given node
   */
  public void killNode(MiniClusterNode node) {
    if (node != null) {
      System.out.println("Killing node: " + node.pid_);
      KillNodeByPid(node.pid_);
    }
    clusterList_.remove(node);
  }

  /**
   * Adds a recordserviced to the cluster. If 'startPlanner' is true, run it as
   * planner; if 'startWorker' is true, run it as worker. If both are true, run
   * as both planner and worker. Returns the new node added.
   */
  public MiniClusterNode addRecordServiced(boolean startPlanner, boolean startWorker) {
    Preconditions.checkArgument(startPlanner || startWorker);
    int pid = AddRecordServiceNode(startPlanner, startWorker);
    Map<String, Integer> args = processNodeArgs(GetNodeArgs(pid));
    args.put("pid", pid);
    MiniClusterNode newNode = new MiniClusterNode(args);
    clusterList_.add(newNode);
    return newNode;
  }

  /**
   * Adds a recordserviced which runs as both planner and worker to the cluster
   */
  public void addRecordServiced() {
    addRecordServiced(true, true);
  }

  public void addNode() {
    addRecordServiced();
  }

  /**
   * Given a planner port, this method returns the process id of the node on the
   * local minicluster
   */
  public int getSpecificNodePid(int plannerPort) {
    return GetSpecificNodePid(plannerPort);
  }

  /**
   * This method returns the number of nodes in the cluster
   */
  public int getClusterSize() {
    return clusterList_.size();
  }

  /**
   * This method returns a randomly selected, live MiniClusterNode object
   */
  public MiniClusterNode getRandomNode() {
    // If the cluster is empty, there are no nodes to return
    if (clusterList_ == null) {
      return null;
    }
    Random r = new Random();
    return clusterList_.get(r.nextInt(clusterList_.size()));
  }

  /**
   * This method kills a randomly selected live node
   */
  public void killRandomNode() {
    killNode(getRandomNode());
  }

  /**
   * This method returns a JobConf object that allows a map reduce job to be run
   * on the minicluster
   */
  public JobConf getJobConf(Class<?> mrClass) {
    if (clusterList_.size() == 0) {
      System.err.println("Cannot run MR job because the cluster has no active nodes");
      return null;
    }
    JobConf conf = new JobConf(mrClass);
    conf.set(RecordServiceConfig.ConfVars.PLANNER_HOSTPORTS_CONF.name, "localhost:"
        + getRandomNode().plannerPort_);
    return conf;
  }

  /**
   * This method takes JobConf and executes it
   */
  public RunningJob runJob(JobConf mrJob) throws IOException {
    if (clusterList_.size() == 0) {
      System.err.println("Cannot run MR job because the cluster has no active nodes");
      return null;
    }
    mrJob.set(RecordServiceConfig.ConfVars.PLANNER_HOSTPORTS_CONF.name,
        "localhost:" + getRandomNode().plannerPort_);
    System.out.println("Running Job");
    return JobClient.runJob(mrJob);
  }

  /**
   * Print all the active nodes to the standard out.
   */
  public void printActiveNodes() {
    for (MiniClusterNode n : clusterList_) {
      System.out.println(n);
    }
  }

  /**
   * This method checks the current state of the MiniClusterController object
   * against the actual state of the system. Returns false if some running
   * cluster nodes are not tracked by this MiniClusterController, or if some
   * nodes tracked by this MiniClusterController are not running. Returns true
   * otherwise.
   */
  public boolean isClusterStateCorrect() {
    HashSet<Integer> pidSet = getRunningMiniNodePids();
    // Check the cluster list
    if (pidSet.size() > 0 && (clusterList_ == null || clusterList_.size() <= 0)) {
      printPids(pidSet,
          "were found but are not being tracked by the MiniClusterController");
      return false;
    } else {
      for (MiniClusterNode node : clusterList_) {
        if (!pidSet.contains(node.pid_)) {
          System.err.println("Node with pid = " + node.pid_
              + " was expected but not found");
          return false;
        }
        // Two nodes cannot share the same process ID
        pidSet.remove(node.pid_);
      }
      if (pidSet.size() > 0) {
        printPids(pidSet,
            "were found but are not being tracked by the MiniClusterController");
        return false;
      }
    }
    return true;
  }

  /**
   * This class represents a node in the minicluster
   */
  public static class MiniClusterNode extends ClusterNode {
    public int pid_;
    public int beeswaxPort_;
    public int hs2Port_;
    public int bePort_;
    public int webserverPort_;
    public int plannerPort_;
    public int workerPort_;

    public MiniClusterNode(Map<String, Integer> args) {
      super("localhost");
      pid_ = args.get("pid");
      beeswaxPort_ = args.get("beeswax_port");
      hs2Port_ = args.get("hs2_port");
      bePort_ = args.get("be_port");
      webserverPort_ = args.get("recordservice_webserver_port");
      plannerPort_ = args.get("recordservice_planner_port");
      workerPort_ = args.get("recordservice_worker_port");
    }

    @Override
    public String toString() {
      String s = "Node pid: " + pid_ + "\n\tbeeswax port: " + beeswaxPort_;
      s = s + "\n\ths2 port: " + hs2Port_ + "\n\tbe port: " + bePort_;
      s = s + "\n\twebserver port: " + webserverPort_ + "\n\tplanner port: ";
      s = s + plannerPort_ + "\n\tworker port: " + workerPort_;
      return s;
    }
  }

  /**
   * This class is used to start a minicluster within its own thread
   */
  public static class ClusterRunner implements Runnable {
    private int numNodes_;

    public ClusterRunner(int numNodes) {
      numNodes_ = numNodes;
    }

    /**
     * This method is executed when a Thread, given an instance of this class,
     * calls its start method
     */
    @Override
    public void run() {
      MiniClusterController.startMiniCluster(numNodes_);
    }
  }

  /**
   * This method starts a minicluster with the specified number of nodes. This
   * method calls via JNI the cpp StartMiniCluster method which sleeps
   * indefinitely after starting the cluster, so this method will not return
   * unless the cluster is stopped.
   */
  private static void startMiniCluster(int numNodes) {
    String rsHome = System.getenv("RECORD_SERVICE_HOME");
    if (rsHome == null) {
      throw new IllegalStateException(
          "Required environment variable RECORD_SERVICE_HOME is not set.");
    }
    String path = rsHome + BUILT_RS_CODE_LOCATION;
    System.load(path + MINICLUSTER_LIBRARY);
    System.out.println("Number of nodes: " + numNodes);
    MiniClusterController.StartMiniCluster(numNodes);
  }

  /**
   * This method starts a minicluster in a new thread
   */
  private void start(int numNodes) throws InterruptedException {
    ClusterRunner cr = new ClusterRunner(numNodes);
    clusterThread_ = new Thread(cr);
    clusterThread_.start();
    System.out.println("Sleeping...");
    // The cluster takes some time to start up
    Thread.sleep(5000);
    populateFields();
  }

  /**
   * This method populates the class fields of the minicluster
   */
  private void populateFields() {
    miniCluster_ = this;
    populateNodeList();
  }

  /**
   * This method takes as input a String array of command line arguments in the
   * form --<arg name>=<arg integer value> and inserts them into a
   * Map<ArgName,ArgValue>.
   */
  private Map<String, Integer> processNodeArgs(String[] args) {
    Map<String, Integer> argTable = new HashMap<String, Integer>();
    if (args == null) {
      return argTable;
    }
    int equalsIndex;
    for (int i = 0; i < args.length; ++i) {
      if (args[i].startsWith("--") && ((equalsIndex = args[i].indexOf("=")) != -1)) {
        String key = args[i].substring(2, equalsIndex);
        try {
          Integer value = Integer.parseInt(args[i].substring(equalsIndex + 1));
          argTable.put(key, value);
        } catch (NumberFormatException nfe) {
          nfe.printStackTrace();
          continue;
        }
      }
    }
    return argTable;
  }

  private void printPids(HashSet<Integer> pidSet, String trailingMessage) {
    System.err.println("Nodes with the following pids: ");
    for (Integer pid : pidSet) {
      System.err.println(pid);
    }
    System.err.println(trailingMessage);
  }

  /**
   * This method populates the node list with the live nodes found running
   */
  private void populateNodeList() {
    clusterList_ = new ArrayList<MiniClusterNode>();
    int[] nodePids = GetRunningMiniNodePids();
    for (int i = 0; i < nodePids.length; ++i) {
      int pid = nodePids[i];
      Map<String, Integer> args = processNodeArgs(GetNodeArgs(pid));
      args.put("pid", pid);
      MiniClusterNode newNode = new MiniClusterNode(args);
      clusterList_.add(newNode);
    }
  }

  /**
   * This constructor returns a MiniClusterController object. Note that method
   * does not start a minicluster. To start a cluster, miniCluster.start()
   * should be called.
   */
  private MiniClusterController(int numNodes) throws InterruptedException {
    super(true, "localhost");
    start(numNodes);
    populateFields();

    // We allot enough time for each node to have 20 seconds to startup. The
    // nodes should not need that long, but this helps prevent test failures
    // due to a slow network
    int timer = 20 * numNodes;
    while (getClusterSize() < numNodes) {
      if (timer == 0) {
        return;
      }
      Thread.sleep(1000);
      populateFields();
      timer--;
    }
  }

  public static void main(String[] args) throws InterruptedException, IOException,
      NumberFormatException {
    org.apache.log4j.BasicConfigurator.configure();
    int numNodes = DEFAULT_NUM_NODES;
    if (args.length == 1) {
      numNodes = Integer.parseInt(args[0]);
    }
    MiniClusterController.Start(numNodes);
    MiniClusterController miniCluster = MiniClusterController.instance();
    System.out.println(miniCluster.isClusterStateCorrect());
  }
}
