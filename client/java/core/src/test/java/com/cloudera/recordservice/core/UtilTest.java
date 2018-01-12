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

import java.nio.ByteBuffer;

import org.junit.Test;

/**
 * Tests for utility classes.
 */
public class UtilTest {
  @Test
  public void byteArrayTest() {
    ByteBuffer buffer = ByteBuffer.allocate(100);
    for (int i = 0; i < 100; ++i) {
      buffer.array()[i] = (byte)i;
    }

    ByteArray ba = new ByteArray(buffer, 0, 100);
    assertEquals(100, ba.len());
    assertEquals(0, ba.offset());
    assertEquals(buffer, ba.byteBuffer());
    for (int i = 0; i < 100; ++i) {
      assertEquals((byte)i, ba.byteBuffer().array()[i]);
    }

    ba = new ByteArray(buffer, 10, 20);
    assertEquals(20, ba.len());
    assertEquals(10, ba.offset());
    assertEquals(buffer, ba.byteBuffer());
    for (int i = 0; i < ba.len(); ++i) {
      assertEquals((byte)i + ba.offset(), ba.byteBuffer().array()[i + ba.offset()]);
    }
  }

  @Test
  public void decimalTest() {
    assertEquals(4, Decimal.computeByteSize(1, 0));
    assertEquals(8, Decimal.computeByteSize(10, 1));
    assertEquals(16, Decimal.computeByteSize(30, 1));

    boolean exceptionThrown = false;
    try {
      Decimal.computeByteSize(40, 0);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Max supported precision"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      Decimal.computeByteSize(1, -1);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("must be non-negative"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      Decimal.computeByteSize(10, 12);
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage(), e.getMessage().contains("Scale cannot be"));
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);

    Decimal decimal = new Decimal(10, 2);
    assertEquals(10, decimal.getPrecision());
    assertEquals(2, decimal.getScale());
    assertEquals(Decimal.computeByteSize(10, 2), decimal.getBytes().length);
  }

  @Test
  public void timestampNanosTest() {
    TimestampNanos t = new TimestampNanos();
    t.set(1420070400000L, 100);
    assertEquals(1420070400000L, t.getMillisSinceEpoch());
    assertEquals(100, t.getNanos());
  }

  @Test
  public void codeCoverageTest() {
    // Trivial "tests" to make code coverage happy.
    new WorkerClientUtil();

    assertEquals("1.0", ProtocolVersion.CLIENT_VERSION);
  }
}
