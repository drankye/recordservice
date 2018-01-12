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

import org.junit.BeforeClass;

public class TestBase {
  protected static final String PLANNER_HOST =
      System.getenv("RECORD_SERVICE_PLANNER_HOST") != null ?
          System.getenv("RECORD_SERVICE_PLANNER_HOST") : "localhost";
  protected static final int PLANNER_PORT =
      System.getenv("RECORD_SERVICE_PLANNER_PORT") != null ?
          Integer.parseInt(System.getenv("RECORD_SERVICE_PLANNER_PORT")) : 12050;

  // Most tests should use the worker port returned from the plan. Only use this
  // for tests that are testing worker connections specifically.
  protected static final int DEFAULT_WORKER_PORT = 13050;
  protected static final String DEFAULT_TEST_TABLE = "tpch.nation";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
  }
}
