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
import java.util.Collection;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.recordservice.tests.JobQueue.RSMRJob;
import com.cloudera.recordservice.tests.ClusterController;

public class LongHaul {

  public static <T> void main(String[] args) throws InterruptedException {
    org.apache.log4j.BasicConfigurator.configure();
    ClusterController cluster = new ClusterController(false, "vd0214.halxg.cloudera.com");
    System.out.println("testLonghaul");
    JobQueue jobQ = new JobQueue(1);
    Collection<Callable<T>> jobList = new ArrayList<Callable<T>>();
    for (int i = 0; i < 350; ++i) {
      jobList.add(jobQ.new RSMRJob(TestMiniClusterController.createWordCountMRJobConf()));
    }
    jobQ.addJobsToQueue(jobList);
    System.out.println(jobQ.checkCompleted());
    System.out.println("testLonghaul end");
    jobQ.killQueue();
  }
}
