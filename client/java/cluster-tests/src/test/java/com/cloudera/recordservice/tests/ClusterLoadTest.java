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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.recordservice.tests.JobQueue.RSMRJob;
import com.cloudera.recordservice.tests.ClusterController;

/**
 * This class submits batches of jobs to a RecordService cluster object using
 * the JobQueue object.
 */
public class ClusterLoadTest {
  public static final int DEFAULT_CLUSTER_NODE_NUM = 3;
  public static final int DEFAULT_WORKER_THREAD_NUM = 3;

  public static String HOST_ADDRESS = System.getenv("HOST_ADDRESS");

  ClusterController cluster_;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    org.apache.log4j.BasicConfigurator.configure();
    if ( HOST_ADDRESS == null || HOST_ADDRESS.equals("")) {
      HOST_ADDRESS = "vd0214.halxg.cloudera.com";
    }
    new ClusterController(false, HOST_ADDRESS);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    cluster_ = ClusterController.cluster_;
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public <T> void testLonghaul() throws InterruptedException, MalformedURLException {
    JobQueue jobQ = new JobQueue(1);
    Collection<Callable<T>> jobList = new ArrayList<Callable<T>>();
    for (int i = 0; i < 10; ++i) {
      jobList.add(jobQ.new RSMRJob(cluster_.populateJobConf(TestMiniClusterController
          .createWordCountMRJobConf())));
    }
    jobQ.addJobsToQueue(jobList);
    assertTrue(jobQ.checkCompleted());
  }
}
