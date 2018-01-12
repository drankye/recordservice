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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.recordservice.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.recordservice.core.PlanRequestResult;
import com.cloudera.recordservice.core.RecordServiceException;
import com.cloudera.recordservice.core.RecordServicePlannerClient;
import com.cloudera.recordservice.core.Request;
import com.cloudera.recordservice.core.TestBase;
import com.cloudera.recordservice.core.WorkerClientUtil;

/**
 * Test the functionality of KeyValueRecords.
 */
public class TestKeyValueRecords extends TestBase {
  /**
   * Test key / value records in table rs.users.
   */
  @Test
  public void testUserKeyValue() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from rs.users"));
    assertEquals(6, plan.tasks.size());

    KeyValueRecords<UserKey, UserValue> records = null;

    for (int i = plan.tasks.size() - 1; i >= 0; --i) {
      try {
        records = new KeyValueRecords<UserKey, UserValue>(UserKey.SCHEMA$,
            UserValue.SCHEMA$, WorkerClientUtil.execTask(plan, i));
        if (records.hasNext()) {
          KeyValueRecords.KeyValuePair<UserKey, UserValue> record = records.next();
          int age = record.KEY.getAge().intValue();
          switch (age) {
            case 10:
              assertEquals("red", record.KEY.getFavoriteColor().toString());
              assertEquals(256, record.VALUE.getFavoriteNumber().intValue());
              assertEquals("sun bear", record.VALUE.getFavoriteAnimal().toString());
              break;
            case 14:
              assertEquals("blue", record.KEY.getFavoriteColor().toString());
              assertEquals(51, record.VALUE.getFavoriteNumber().intValue());
              assertEquals("elk", record.VALUE.getFavoriteAnimal().toString());
              break;
            case 21:
              assertEquals("blue", record.KEY.getFavoriteColor().toString());
              assertEquals(96, record.VALUE.getFavoriteNumber().intValue());
              assertEquals("dog", record.VALUE.getFavoriteAnimal().toString());
              break;
            case 30:
              assertEquals("red", record.KEY.getFavoriteColor().toString());
              assertEquals(19, record.VALUE.getFavoriteNumber().intValue());
              assertEquals("cat", record.VALUE.getFavoriteAnimal().toString());
              break;
            case 56:
              assertEquals("red", record.KEY.getFavoriteColor().toString());
              assertEquals(1, record.VALUE.getFavoriteNumber().intValue());
              assertEquals("dog", record.VALUE.getFavoriteAnimal().toString());
              break;
            case 60:
              assertEquals(null, record.KEY.getFavoriteColor());
              assertEquals(22, record.VALUE.getFavoriteNumber().intValue());
              assertEquals(null, record.VALUE.getFavoriteAnimal());
              break;
            default:
              throw new RuntimeException("Unsupported record with age=" + age);
          }
        }
      } finally {
        if (records != null) records.close();
      }
    }
  }

  /**
   * Test table rs.nullUsers with null column value - 'age', and the key schema does not
   * set the default value for the null column.
   */
  @Test
  public void testNullUsers() throws IOException, RecordServiceException {
    PlanRequestResult plan = new RecordServicePlannerClient.Builder()
        .planRequest(PLANNER_HOST, PLANNER_PORT,
            Request.createSqlRequest("select * from rs.nullUsers"));
    assertEquals(1, plan.tasks.size());

    KeyValueRecords<UserKey, UserValue> records = new KeyValueRecords<UserKey, UserValue>(
        UserKey.SCHEMA$, UserValue.SCHEMA$, WorkerClientUtil.execTask(plan, 0));
    if (records.hasNext()) {
      boolean exceptionThrown = false;
      try {
        records.next();
      } catch (RuntimeException e) {
        exceptionThrown = true;
        // Throw exception as age is null, and default value is not set.
        assertTrue(e.getMessage().contains("Default value is not set"));
      } finally {
        assertEquals(true, exceptionThrown);
        records.close();
      }
    }
  }
}
