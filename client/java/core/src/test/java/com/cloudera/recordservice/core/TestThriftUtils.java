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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test functions in ThriftUtils.
 */
public class TestThriftUtils extends TestBase {

  @Test
  public void testIsValidVersionFormat() {
    assertTrue(ThriftUtils.isValidVersionFormat("1.0"));
    assertTrue(ThriftUtils.isValidVersionFormat("10.3"));
    assertFalse(ThriftUtils.isValidVersionFormat("-1.0"));
  }

  @Test
  public void testNonValidProtocolVersion() {
    boolean exceptionThrown = false;
    try {
      ThriftUtils.fromThrift("+2.0");
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Unrecognized version format: +2.0"));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      ThriftUtils.fromThrift("1.");
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Unrecognized version format: 1."));
    } finally {
      assertTrue(exceptionThrown);
    }

    exceptionThrown = false;
    try {
      ThriftUtils.fromThrift(".1");
    } catch (RuntimeException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Unrecognized version format: .1"));
    } finally {
      assertTrue(exceptionThrown);
    }
  }

  @Test
  public void testValidProtocolVersion() {
    assertEquals("2.0", ThriftUtils.fromThrift("2.0").getVersion());
    assertEquals("3.1", ThriftUtils.fromThrift("3.1").getVersion());
  }
}
