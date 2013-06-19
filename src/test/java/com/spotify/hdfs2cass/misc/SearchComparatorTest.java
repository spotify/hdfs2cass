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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author anand
 */
public class SearchComparatorTest {

  @Test
  public void testCompare() throws Exception {
    SearchComparator comparator = new SearchComparator();
    TokenNode instance = new TokenNode("2:5");

    assertEquals(1, comparator.compare(instance, new TokenNode(("0:1"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("1:3"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("3:4"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("2:5"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("3:5"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("3:0"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("3:6"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("6:8"))));
  }

  @Test
  public void testCompareWraparound() throws Exception {
    SearchComparator comparator = new SearchComparator();
    TokenNode instance = new TokenNode("5:2");

    assertEquals(1, comparator.compare(instance, new TokenNode(("0:1"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("3:4"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("2:5"))));
    assertEquals(1, comparator.compare(instance, new TokenNode(("3:0"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("5:6"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("6:8"))));
    assertEquals(0, comparator.compare(instance, new TokenNode(("7:1"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("6:3"))));
    assertEquals(-1, comparator.compare(instance, new TokenNode(("8:4"))));
  }
}
