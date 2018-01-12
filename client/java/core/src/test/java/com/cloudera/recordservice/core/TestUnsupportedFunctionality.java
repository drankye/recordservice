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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

/**
 * Tests that the RecordService does not support these kind of queries.
 */
public class TestUnsupportedFunctionality extends TestBase {

  private void testUnsupported(String sql) {
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .planRequest(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest(sql));
    } catch (IOException e) {
      assertFalse(e.getMessage(), true);
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Could not plan request."));
      assertTrue(e.detail,
          e.detail.contains("RecordService only supports scan queries."));
    }
    assertTrue("Request should not be supported: " + sql, exceptionThrown);
  }

  @Test
  public void testUnsupported() {
    testUnsupported("SELECT 1");
    testUnsupported("SELECT min(n_nationkey) FROM tpch.nation");
    testUnsupported("SELECT count(*), min(n_nationkey) FROM tpch.nation");
    testUnsupported("SELECT count(n_nationkey) FROM tpch.nation");
    testUnsupported("SELECT count(*) FROM tpch.nation group by n_nationkey");
    testUnsupported("SELECT t1.* FROM tpch.nation as t1 JOIN tpch.nation as t2 "+
        "ON t1.n_nationkey = t2.n_nationkey");
    testUnsupported("SELECT * from tpch.nation LIMIT 1");
    testUnsupported("use default");
    testUnsupported("set num_nodes=1");
    testUnsupported("explain select * from tpch.nation");
    testUnsupported("create table rs.not_exists(i int)");
    testUnsupported("invalidate metadata");
    testUnsupported("invalidate metadata tpch.nation");
    testUnsupported("drop table tpch.nation");
    testUnsupported("create view rs.test_vs as select * from tpch.nation");


    // VM doesn't support write path access
    if (!TestBasicClient.RECORD_SERVICE_QUICKSTART_VM) {
      testUnsupported("insert into tpch.nation select * from tpch.nation");
    }

    // Test getSchema
    boolean exceptionThrown = false;
    try {
      new RecordServicePlannerClient.Builder()
          .getSchema(PLANNER_HOST, PLANNER_PORT, Request.createSqlRequest("select 1"));
    } catch (IOException e) {
      assertFalse(e.getMessage(), true);
    } catch (RecordServiceException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage(), e.getMessage().contains("Could not plan request."));
      assertTrue(e.detail,
          e.detail.contains("RecordService only supports scan queries."));
    }
    assertTrue(exceptionThrown);
  }
}
