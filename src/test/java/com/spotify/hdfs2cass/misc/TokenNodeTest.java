// Copyright (c) 2013 Spotify AB
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.spotify.hdfs2cass.misc;

import org.apache.cassandra.dht.BigIntegerToken;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author anand
 */
public class TokenNodeTest {

  @Test
  public void testConstructor() throws Exception {
    final BigIntegerToken token = new BigIntegerToken("1");
    TokenNode instance = new TokenNode(token);

    assertEquals(token, instance.getStartToken());
    assertEquals(token, instance.getEndToken());
  }

  @Test
  public void testStringConstructor() throws Exception {
    TokenNode instance = new TokenNode("1:2");

    assertEquals(new BigIntegerToken("1"), instance.getStartToken());
    assertEquals(new BigIntegerToken("2"), instance.getEndToken());
  }

  @Test
  public void testEquals() throws Exception {
    TokenNode instance = new TokenNode("1:2");

    assertEquals(new TokenNode("1:2"), instance);
    assertNotEquals(new TokenNode("2:1"), instance);
  }
}
