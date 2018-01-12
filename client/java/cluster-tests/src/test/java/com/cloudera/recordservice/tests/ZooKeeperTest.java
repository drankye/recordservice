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

package com.cloudera.recordservice.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.cloudera.recordservice.core.NetworkAddress;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.mr.RecordServiceConfig;
import com.cloudera.recordservice.mr.ZooKeeperUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static com.cloudera.recordservice.tests.MiniClusterController.MiniClusterNode;

/**
 * This class tests the {@link com.cloudera.recordservice.mr.ZooKeeperUtil} class.
 * In particular, the planner auto discovery feature. This is done by starting up
 * a mini cluster.
 */
public class ZooKeeperTest {
  private static final int PLANNER_NUM = 10;
  private static final int SAMPLE_PLANNER_NUM = 3;

  // Maximum time (in sec) to wait for ZK to update membership info
  private static final int MAX_WAIT_SEC = 120;

  private static MiniClusterController controller_;
  private static Random rand_;

  // The ZK session and cache used to track membership change
  private static CuratorFramework cf_;
  private static PathChildrenCache cache_;

  // A single threaded executor used for running futures waiting for ZK event
  private static final ExecutorService executor_ = Executors.newSingleThreadExecutor();

  private static final String testConnectionStr = "localhost:2181";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    org.apache.log4j.BasicConfigurator.configure();
    MiniClusterController.Start(0);
    controller_ = MiniClusterController.instance();
    rand_ = new Random();
    cf_ = CuratorFrameworkFactory.builder()
        .connectString(testConnectionStr)
        .connectionTimeoutMs(30 * 1000)
        .aclProvider(new TestACLProvider())
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .build();
    cf_.start();
    cache_ = new PathChildrenCache(cf_,
        RecordServiceConfig.ZOOKEEPER_ZNODE_DEFAULT + "/planners", true);
    cache_.start();
  }

  @AfterClass
  public static void setUpAfterClass() throws Exception {
    cf_.close();
    cache_.getListenable().clear();
    cache_.close();
    executor_.shutdownNow();
    executor_.awaitTermination(1, TimeUnit.MINUTES);
  }

  /**
   * Test ZooKeeper planner membership auto discovery for MR jobs.
   * TODO: add some negative tests
   * TODO: if the user who runs this test has kerberos credentials, then the test
   * will fail. We need to understand the reason.
   */
  @Test
  public void testPlanners() throws IOException, InterruptedException,
      ExecutionException {
    // Add a worker so that planning request won't fail
    controller_.addRecordServiced(false, true);

    List<MiniClusterNode> planners = new ArrayList<MiniClusterNode>();
    for (int i = 0; i < PLANNER_NUM; ++i) {
      planners.add(controller_.addRecordServiced(true, false));
    }

    Configuration conf = new Configuration();
    conf.set(RecordServiceConfig.ConfVars.ZOOKEEPER_CONNECTION_STRING_CONF.name,
        testConnectionStr);
    List<NetworkAddress> plannerAddresses = ZooKeeperUtil.getPlanners(conf);

    RecordServicePlannerClient planner = null;
    for (int i = 0; i < SAMPLE_PLANNER_NUM; ++i) {
      NetworkAddress address = plannerAddresses.get(rand_.nextInt(PLANNER_NUM));
      try {
        planner = new RecordServicePlannerClient.Builder().connect(
            address.hostname, address.port);
        planner.planRequest(Request.createTableScanRequest("tpch.nation"));
      } catch (RecordServiceException e) {
        assertTrue("Expected planning to succeed at address "
            + address + ", but failed with exception " + e, false);
      } finally {
        if (planner != null) planner.close();
      }
    }

    // Now randomly kill a planner node, and we should expect the addresses from
    // ZK to match the updated planner nodes.
    MiniClusterNode nodeToKill = planners.get(rand_.nextInt(PLANNER_NUM));

    // We need to create the future first and then check the future. This is because
    // the 'killNode' call also waits for certain amount of time, during which
    // the ZK event may already triggered.
    Future<Void> future = createEventFuture(Type.CHILD_REMOVED);
    controller_.killNode(nodeToKill);
    planners.remove(nodeToKill);
    checkEventFuture(future, Type.CHILD_REMOVED);
    plannerAddresses = ZooKeeperUtil.getPlanners(conf);
    checkAlivePlanners(plannerAddresses, planners);

    // Make sure the killed node is not in ZK addresses
    for (NetworkAddress addr : plannerAddresses) {
      if (addr.hostname.equals(nodeToKill.hostname_) &&
          addr.port == nodeToKill.plannerPort_) {
        assertFalse("Expected node at port " + nodeToKill.plannerPort_
            + " to be killed, but still found it in the ZK addresses.", true);
      }
    }

    // Attempt to plan on the killed node should fail
    try {
      planner = new RecordServicePlannerClient.Builder().connect(
          nodeToKill.hostname_, nodeToKill.plannerPort_);
      planner.planRequest(Request.createTableScanRequest("tpch.nation"));
      assertTrue("Expected planning on the killed node:\n" + nodeToKill
          + "\nto fail, but it still succeeded.", false);
    } catch (IOException e) {
      // Do nothing
    } catch (RecordServiceException e) {
      assertTrue("Expected IOException, but got: " + e.getClass().getName(), false);
    } finally {
      if (planner != null) planner.close();
    }

    // Adding a new planner node. The ZK addresses should also be updated accordingly
    future = createEventFuture(Type.CHILD_ADDED);
    MiniClusterNode nodeToAdd = controller_.addRecordServiced(true, false);
    planners.add(nodeToAdd);
    checkEventFuture(future, Type.CHILD_ADDED);
    checkAlivePlanners(ZooKeeperUtil.getPlanners(conf), planners);

    // We should also be able to plan request on the new node
    try {
      planner = new RecordServicePlannerClient.Builder().connect(
          nodeToAdd.hostname_, nodeToAdd.plannerPort_);
      planner.planRequest(Request.createTableScanRequest("tpch.nation"));
    } catch (RecordServiceException e) {
      assertTrue("Expected planning to succeed at address "
          + nodeToAdd.plannerPort_ + ", but failed with exception " + e, false);
    } finally {
      if (planner != null) planner.close();
    }
  }

  /**
   * Check whether the 'plannerAddresses' monitored by ZK include all addresses
   * of planners in 'planners' from the mini cluster.
   */
  private void checkAlivePlanners(List<NetworkAddress> plannerAddresses,
                                  List<MiniClusterNode> planners) {
    List<MiniClusterNode> plannersCopy = new ArrayList<MiniClusterNode>(planners);
    Iterator<MiniClusterNode> it = plannersCopy.iterator();
    while (it.hasNext()) {
      MiniClusterNode node = it.next();
      for (NetworkAddress addr : plannerAddresses) {
        if (addr.hostname.equals(node.hostname_) && addr.port == node.plannerPort_) {
          it.remove();
        }
      }
    }
    assertEquals("Found " + plannersCopy.size() + " that are not in the addresses" +
        " tracked by ZooKeeper", 0, plannersCopy.size());
  }

  /**
   * Returns a future that waits for the particular 'eventType' to happen in the
   * path tracked by 'cache_'.
   */
  private Future<Void> createEventFuture(final Type eventType)
      throws InterruptedException {
    final Status status = new Status();
    PathChildrenCacheListener listener = new PathChildrenCacheListener() {
      @Override
      public void childEvent(CuratorFramework cf, PathChildrenCacheEvent event)
          throws Exception {
        if (event.getType() == eventType) {
          status.done = true;
        }
      }
    };
    cache_.getListenable().clear();
    cache_.getListenable().addListener(listener);

    return executor_.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // Wait infinitely until the event is triggered
        while (!status.done) {
          TimeUnit.SECONDS.sleep(1);
        }
        return null;
      }
    });
  }

  // Wait until the future finished. Fail the test if timeout.
  private void checkEventFuture(Future<Void> future, Type eventType)
      throws ExecutionException, InterruptedException {
    try {
      future.get(MAX_WAIT_SEC, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      future.cancel(true);
      assertFalse("The event " + eventType + " didn't happen.", true);
    }
  }

  private static class Status {
    volatile boolean done = false;
  }

  private static class TestACLProvider implements ACLProvider {
    @Override
    public List<ACL> getDefaultAcl() {
      return ZooDefs.Ids.READ_ACL_UNSAFE;
    }
    @Override
    public List<ACL> getAclForPath(String s) {
      return getDefaultAcl();
    }
  }

}
