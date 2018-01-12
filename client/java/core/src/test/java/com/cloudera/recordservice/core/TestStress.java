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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

// Tests the clients in stressful environments.
public class TestStress extends TestBase {
  @Test
  public void testPlannerConnections() throws RuntimeException, IOException,
        RecordServiceException, InterruptedException {
    // This is more than the maximum number of client threads.
    // TODO: fix the thrift connections to not service the request on the
    // connection thread? This might be hard to do.
    int numConnections = 75;
    List<RecordServicePlannerClient> clients =
        new ArrayList<RecordServicePlannerClient>();

    boolean gotServerBusy = false;
    for (int i = 0; i < numConnections;) {
      ++i;
      try {
        RecordServicePlannerClient planner = new RecordServicePlannerClient.Builder()
            .setSleepDurationMs(10)
            .connect(PLANNER_HOST, PLANNER_PORT);
        clients.add(planner);
      } catch(RecordServiceException ex) {
        assertEquals(RecordServiceException.ErrorCode.SERVICE_BUSY, ex.code);
        gotServerBusy = true;

        // Closing an existing connection should work.
        assertTrue(clients.size() > 0);
        RecordServicePlannerClient c = clients.remove(0);
        c.close();
        // TODO: we need a sleep to because close is processed asynchronously
        // somewhere in thrift. This is kind of a hack but only marginally as
        // a reasonable client should sleep before retrying.
        Thread.sleep(200); // ms

        c = new RecordServicePlannerClient.Builder().connect(PLANNER_HOST, PLANNER_PORT);
        clients.add(c);
      }
    }

    for (RecordServicePlannerClient c: clients) {
      c.close();
    }

    // If this fails, increase numConnections. It must be larger than what the
    // service is configured to (default = 64).
    assertTrue(gotServerBusy);
  }

  @Test
  public void testWorkerConnections() throws RuntimeException, IOException,
      RecordServiceException, InterruptedException {
    // This is more than the maximum number of client threads.
    // TODO: fix the thrift connections to not service the request on the
    // connection thread? This might be hard to do.
    int numConnections = 75;
    List<RecordServiceWorkerClient> clients = Lists.newArrayList();

    boolean gotServerBusy = false;
    for (int i = 0; i < numConnections; ++i) {
      try {
        RecordServiceWorkerClient worker = new RecordServiceWorkerClient.Builder()
            .setSleepDurationMs(10)
            .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);
        clients.add(worker);
      } catch(RecordServiceException ex) {
        assertEquals(RecordServiceException.ErrorCode.SERVICE_BUSY, ex.code);
        gotServerBusy = true;

        // Closing an existing connection should work.
        assertTrue(clients.size() > 0);
        RecordServiceWorkerClient c = clients.remove(0);
        c.close();
        Thread.sleep(200);

        c = new RecordServiceWorkerClient.Builder()
            .connect(PLANNER_HOST, DEFAULT_WORKER_PORT);
        clients.add(c);
      }
    }

    for (RecordServiceWorkerClient c: clients) {
      c.close();
    }

    // If this fails, increase numConnections. It must be larger than what the
    // service is configured to (default = 64).
    assertTrue(gotServerBusy);
  }
}
