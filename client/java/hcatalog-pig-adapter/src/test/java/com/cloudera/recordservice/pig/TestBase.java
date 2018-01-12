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

package com.cloudera.recordservice.pig;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.BeforeClass;

import java.util.Properties;

public class TestBase {
  protected static PigServer server;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", "thrift://localhost:9083");
    server = new PigServer(ExecType.LOCAL, properties);
  }
}


