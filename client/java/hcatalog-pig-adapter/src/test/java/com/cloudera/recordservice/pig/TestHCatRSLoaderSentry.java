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

import java.io.IOException;

import com.cloudera.recordservice.core.RecordServiceException;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class tests the scenario when using HCatalog/Pig + RS + Sentry.
 */
public class TestHCatRSLoaderSentry extends TestBase {
  private static final boolean IGNORE_SENTRY_TESTS =
      System.getenv("IGNORE_SENTRY_TESTS") == null ||
          System.getenv("IGNORE_SENTRY_TESTS").equalsIgnoreCase("true");

  @Test
  public void testNation() throws IOException, RecordServiceException {
    if (IGNORE_SENTRY_TESTS) return;

    // Try to select all the columns - this should fail
    try {
      server.registerQuery("A = LOAD 'tpch.nation' " +
          "USING com.cloudera.recordservice.pig.HCatRSLoader();");
      server.openIterator("A");
      assertTrue("Should have thrown an exception", false);
    } catch (FrontendException ex) {
      // Check the exact error type: 2 means INPUT, 1066 means "Unable to
      // open iterator for alias 'A'".
      assertEquals(2, ex.getErrorSource());
      assertEquals(1066, ex.getErrorCode());
      assertTrue(ex.getCause() instanceof IOException);
      // TODO: seems the AuthorizationException is thrown at the backend and
      // doesn't populate to here. How do we verify this is the correct exception?
      // Do nothing
    }

    // Try to select some columns that the user doesn't have privilege to access.
    // This should fail as well.
    try {
      server.registerQuery("A = LOAD 'select n_name, n_comment from tpch.nation' " +
          "USING com.cloudera.recordservice.pig.HCatRSLoader();");
      server.openIterator("A");
      assertTrue("Should have thrown an exception", false);
    } catch (FrontendException ex) {
      assertEquals(2, ex.getErrorSource());
      assertEquals(1066, ex.getErrorCode());
      assertTrue(ex.getCause() instanceof IOException);
    }

    // Access those columns that the user has been granted privilege to should succeed
    try {
      server.registerQuery("A = LOAD 'select n_nationkey, n_name from tpch.nation' " +
          "USING com.cloudera.recordservice.pig.HCatRSLoader();");
      server.openIterator("A");
    } catch (FrontendException ex) {
      assertTrue("Accessing columns with correct privilege should not " +
          "throw exception", false);
    }
  }

}
