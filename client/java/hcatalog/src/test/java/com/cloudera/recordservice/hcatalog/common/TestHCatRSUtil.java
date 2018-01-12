/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cloudera.recordservice.hcatalog.common;

import com.cloudera.recordservice.core.TestBase;
import com.cloudera.recordservice.hcatalog.mapreduce.InputJobInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestHCatRSUtil extends TestBase {
  @Test
  public void serializeTest() throws IOException {
    assertEquals("", HCatRSUtil.serialize(null));

    String result = HCatRSUtil.serialize(
        InputJobInfo.create("db", "example", "filter", null));
    assertTrue(result != null);
  }

  @Test
  public void deserializeTest() throws IOException {
    // first the null branches
    assertEquals(null, HCatRSUtil.deserialize(null));
    assertEquals(null, HCatRSUtil.deserialize(""));
  }

  @Test
  public void encodeBytesTest() {
    byte[] test = "testEncoding".getBytes();
    String result = HCatRSUtil.encodeBytes(test);
    assertTrue(result != null);
  }

  @Test
  public void decodeBytesTest() {
    byte[] result = HCatRSUtil.decodeBytes("testDecoding");
    assertTrue(result != null);
  }

  @Test
  public void copyCredentialsToJobConfTest() {
    JobConf conf = new JobConf();
    Credentials cred = new Credentials();
    cred.addToken(new Text("Alias"), new Token<TokenIdentifier>());
    HCatRSUtil.copyCredentialsToJobConf(cred, conf);
    assertEquals(1, conf.getCredentials().numberOfTokens());
  }
}
